import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { updateRoleSchema } from '../_shared/schemas.ts';

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    // Get the requesting user
    const authHeader = req.headers.get("Authorization")!;
    const token = authHeader.replace("Bearer ", "");
    const { data: { user }, error: authError } = await supabase.auth.getUser(token);

    if (authError || !user) {
      return new Response(
        JSON.stringify({ error: "Unauthorized" }),
        { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Check if requesting user is admin using secure user_roles table
    const { data: userRoles, error: roleError } = await supabase
      .from("user_roles")
      .select("role")
      .eq("user_id", user.id);

    if (roleError || !userRoles || userRoles.length === 0) {
      return new Response(
        JSON.stringify({ error: "Admin access required" }),
        { status: 403, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const hasAdminRole = userRoles.some(roleRecord =>
      roleRecord.role === 'admin' || roleRecord.role === 'developer'
    );

    if (!hasAdminRole) {
      return new Response(
        JSON.stringify({ error: "Admin access required" }),
        { status: 403, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Parse and validate request body
    const requestBody = await req.json();
    const validationResult = updateRoleSchema.safeParse(requestBody);
    if (!validationResult.success) {
      type ZodIssue = { path: Array<string | number>; message: string };
      const issues = (validationResult as unknown as { error: { issues: ZodIssue[] } }).error.issues as ZodIssue[];
      return new Response(
        JSON.stringify({
          error: 'Validation failed',
          details: issues.map(e => ({ field: e.path.join('.'), message: e.message }))
        }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const { userId, newRole } = validationResult.data;

    // Update role in users table (this will trigger sync to user_roles table)
    const { error: updateError } = await supabase
      .from("users")
      .update({ role: newRole })
      .eq("id", userId);

    if (updateError) {
      console.error("Error updating users table:", updateError);
      return new Response(
        JSON.stringify({ error: "Failed to update role in database" }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Explicitly update user_roles table (backup in case trigger fails)
    // First delete old role
    await supabase
      .from("user_roles")
      .delete()
      .eq("user_id", userId);

    // Then insert new role
    const { error: userRolesError } = await supabase
      .from("user_roles")
      .insert({ user_id: userId, role: newRole });

    if (userRolesError) {
      console.error("Error updating user_roles table:", userRolesError);
      // Don't fail the request since the users table was updated and trigger should handle it
      console.warn("Role updated in users table but user_roles sync failed");
    }
    // Get current user metadata
    const { data: targetUser, error: getUserError } = await supabase.auth.admin.getUserById(userId);

    if (getUserError || !targetUser) {
      console.error("Error getting user:", getUserError);
      return new Response(
        JSON.stringify({ error: "Failed to get user" }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Update user metadata in auth
    const { error: metadataError } = await supabase.auth.admin.updateUserById(userId, {
      user_metadata: {
        ...targetUser.user.user_metadata,
        role: newRole,
      },
    });

    if (metadataError) {
      console.error("Error updating user metadata:", metadataError);
      // Don't fail the request since the database was updated
      console.warn("Role updated in database but metadata sync failed");
    }

    console.log(`✅ Role updated for user ${userId}: ${newRole}`);

    return new Response(
      JSON.stringify({
        success: true,
        message: "Role updated successfully",
        userId,
        newRole,
      }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (error) {
    console.error("Error in update-user-role function:", error);
    return new Response(
      JSON.stringify({ error: "Internal server error" }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});
