
import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'
import { triggerN8nWebhook } from '../_shared/n8nWebhook.ts';
import { logAdminAction } from '../_shared/auditLog.ts';
import { orderManagementSchema } from '../_shared/schemas.ts';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders })
  }

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    const supabase = createClient(supabaseUrl, supabaseServiceKey)

    // Verify authentication
    const authHeader = req.headers.get('Authorization');
    if (!authHeader) {
      return new Response(
        JSON.stringify({ error: 'Unauthorized: Missing authorization header' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const token = authHeader.replace('Bearer ', '');
    const { data: { user }, error: authError } = await supabase.auth.getUser(token);

    if (authError || !user) {
      return new Response(
        JSON.stringify({ error: 'Unauthorized: Invalid token' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Check if requesting user is admin or developer using user_roles table
    const { data: userRoles, error: roleError } = await supabase
      .from('user_roles')
      .select('role')
      .eq('user_id', user.id);

    const roles = userRoles?.map(r => r.role) || [];
    const isAdmin = roles.includes('admin') || roles.includes('developer');

    if (roleError || !isAdmin) {
      return new Response(
        JSON.stringify({ error: 'Forbidden: Admin access required' }),
        { status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const requestBody = await req.json();

    // Validate input with Zod schema
    const validationResult = orderManagementSchema.safeParse(requestBody);
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

    const operation = validationResult.data.operation;
    const params = validationResult.data as unknown as {
      id?: string;
      payment_status?: 'paid' | 'unpaid' | 'pending' | 'refunded';
      order_status?: 'pending' | 'processing' | 'completed' | 'cancelled';
      transaction_id?: string | null;
      payment_mode?: string | null;
      paid_at?: string | null;
      updated_by?: string;
    };

    // Get IP and User Agent for audit logging
    const ipAddress = req.headers.get('x-forwarded-for') || req.headers.get('x-real-ip') || 'unknown';
    const userAgent = req.headers.get('user-agent') || 'unknown';

    switch (operation) {
      case 'fetch_orders': {
        // Audit log: Admin accessing all orders with PII
        await logAdminAction({
          supabase,
          userId: user.id,
          userRole: roles.includes('admin') ? 'admin' : 'developer',
          action: 'SELECT',
          tableName: 'orders',
          newValues: { operation: 'fetch_orders', note: 'Admin accessed all orders with customer PII' },
          ipAddress,
          userAgent
        });

        const { data: orders, error } = await supabase
          .from('orders')
          .select(`
            *,
            users!orders_user_id_fkey(student_id, name),
            order_items(
              *,
              products(name)
            )
          `)
          .order('created_at', { ascending: false })

        if (error) throw error

        return new Response(
          JSON.stringify({ orders }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        )
      }

      case 'get_stats': {
        // Audit log: Admin accessing financial metrics
        await logAdminAction({
          supabase,
          userId: user.id,
          userRole: roles.includes('admin') ? 'admin' : 'developer',
          action: 'SELECT',
          tableName: 'orders',
          newValues: { operation: 'get_stats', note: 'Admin accessed revenue and financial metrics' },
          ipAddress,
          userAgent
        });

        const today = new Date().toISOString().split('T')[0]

        // Today's orders count
        const { count: todayOrders } = await supabase
          .from('orders')
          .select('*', { count: 'exact', head: true })
          .gte('created_at', `${today}T00:00:00`)
          .lt('created_at', `${today}T23:59:59`)

        // Today's revenue
        const { data: revenueData } = await supabase
          .from('orders')
          .select('total_amount')
          .gte('created_at', `${today}T00:00:00`)
          .lt('created_at', `${today}T23:59:59`)
          .eq('payment_status', 'paid')

        interface RevenueRow { total_amount: number }
        const revenue = (revenueData as RevenueRow[] | null)?.reduce((sum: number, order: RevenueRow) => sum + order.total_amount, 0) || 0

        // Pending orders
        const { count: pendingOrders } = await supabase
          .from('orders')
          .select('*', { count: 'exact', head: true })
          .eq('payment_status', 'unpaid')

        // Average order value
        const { data: avgData } = await supabase
          .from('orders')
          .select('total_amount')

        const avgOrder = (avgData as RevenueRow[] | null)?.length
          ? (avgData as RevenueRow[]).reduce((sum: number, order: RevenueRow) => sum + order.total_amount, 0) / (avgData as RevenueRow[]).length
          : 0

        return new Response(
          JSON.stringify({
            todayOrders: todayOrders || 0,
            revenue,
            pendingOrders: pendingOrders || 0,
            avgOrder: Math.round(avgOrder)
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        )
      }

      case 'update_order': {
        const id = params.id;
        const updateData: Record<string, unknown> = { ...(params as Record<string, unknown>) };
        delete updateData.id;
        delete updateData.operation; // Remove operation field from update data

        // Get current order data before update to check status change
        const { data: currentOrder, error: currentOrderError } = await supabase
          .from('orders')
          .select('payment_status')
          .eq('id', id)
          .single()

        if (currentOrderError) throw currentOrderError

        const { data, error } = await supabase
          .from('orders')
          .update({
            ...updateData,
            updated_at: new Date().toISOString(),
            updated_by: params.updated_by
          })
          .eq('id', id)
          .select()

        if (error) throw error

        // Audit log: Admin modified order
        await logAdminAction({
          supabase,
          userId: user.id,
          userRole: roles.includes('admin') ? 'admin' : 'developer',
          action: 'UPDATE',
          tableName: 'orders',
          recordId: id,
          oldValues: { payment_status: currentOrder.payment_status },
          newValues: updateData,
          ipAddress,
          userAgent
        });

        // If payment_status is set to 'paid', trigger n8n webhook
        if (updateData.payment_status === 'paid') {
          await triggerN8nWebhook('points', {
            orderId: id,
            userId: data[0]?.user_id,
            amount: data[0]?.total_amount,
            event: 'payment_completed',
            timestamp: new Date().toISOString()
          });
        }

        return new Response(
          JSON.stringify({
            order: data[0],
            previousStatus: currentOrder.payment_status
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        )
      }

      default:
        return new Response(
          JSON.stringify({ error: 'Invalid operation' }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        )
    }
  } catch (error) {
    console.error('Error:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
    return new Response(
      JSON.stringify({ error: errorMessage }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    )
  }
})
