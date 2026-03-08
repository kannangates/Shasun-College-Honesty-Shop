import { serve } from "std/http";
import { createClient } from '@supabase/supabase-js';
import { stockOperationSchema } from '../_shared/schemas.ts';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Verify authentication
    const authHeader = req.headers.get('Authorization');
    if (!authHeader) {
      return new Response(
        JSON.stringify({ success: false, error: 'Unauthorized: Missing authorization header' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const token = authHeader.replace('Bearer ', '');
    const { data: { user }, error: authError } = await supabase.auth.getUser(token);

    if (authError || !user) {
      return new Response(
        JSON.stringify({ success: false, error: 'Unauthorized: Invalid token' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Check if requesting user is admin using secure user_roles table
    // Use service role client to bypass RLS for role checking
    const supabaseService = createClient(supabaseUrl, supabaseServiceKey);
    const { data: userRoles, error: roleError } = await supabaseService
      .from('user_roles')
      .select('role')
      .eq('user_id', user.id);

    if (roleError) {
      console.error('Error fetching user roles:', roleError);
      return new Response(
        JSON.stringify({ success: false, error: 'Forbidden: Unable to verify role' }),
        { status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const roles = userRoles?.map(r => r.role) || [];
    const isAdmin = roles.includes('admin') || roles.includes('developer');

    const requestBody = await req.json();

    // Validate input with Zod schema
    const validationResult = stockOperationSchema.safeParse(requestBody);
    if (!validationResult.success) {
      type ZodIssue = { path: Array<string | number>; message: string };
      const issues = (validationResult as unknown as { error: { issues: ZodIssue[] } }).error.issues as ZodIssue[];
      return new Response(
        JSON.stringify({
          success: false,
          error: 'Validation failed',
          details: issues.map(e => ({ field: e.path.join('.'), message: e.message }))
        }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const { operation, productId, quantity, source } = validationResult.data;

    // Check permissions based on operation and source
    const isCheckoutOperation = operation === 'adjust_shelf_stock' && source === 'Checkout';

    if (!isAdmin && !isCheckoutOperation) {
      return new Response(
        JSON.stringify({ success: false, error: 'Forbidden: Admin access required for this operation' }),
        { status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    switch (operation) {
      case 'restock_warehouse': {
        // Add stock to warehouse
        const { data: product, error: fetchError } = await supabase
          .from('products')
          .select('warehouse_stock')
          .eq('id', productId)
          .single();

        if (fetchError || !product) {
          return new Response(
            JSON.stringify({ success: false, error: 'Product not found' }),
            { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }

        const newWarehouseStock = (product.warehouse_stock || 0) + quantity;

        const { error: updateError } = await supabase
          .from('products')
          .update({
            warehouse_stock: newWarehouseStock,
            updated_at: new Date().toISOString()
          })
          .eq('id', productId);

        if (updateError) throw updateError;

        return new Response(
          JSON.stringify({
            success: true,
            message: `Added ${quantity} units to warehouse. New warehouse stock: ${newWarehouseStock}`,
            data: { warehouse_stock: newWarehouseStock }
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      case 'restock_shelf': {
        // Move stock from warehouse to shelf
        const { data: product, error: fetchError } = await supabase
          .from('products')
          .select('warehouse_stock, shelf_stock')
          .eq('id', productId)
          .single();

        if (fetchError || !product) {
          return new Response(
            JSON.stringify({ success: false, error: 'Product not found' }),
            { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }

        if ((product.warehouse_stock || 0) < quantity) {
          return new Response(
            JSON.stringify({
              success: false,
              error: `Insufficient warehouse stock. Available: ${product.warehouse_stock || 0}, Requested: ${quantity}`
            }),
            { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }

        const newWarehouseStock = (product.warehouse_stock || 0) - quantity;
        const newShelfStock = (product.shelf_stock || 0) + quantity;

        const { error: updateError } = await supabase
          .from('products')
          .update({
            warehouse_stock: newWarehouseStock,
            shelf_stock: newShelfStock,
            updated_at: new Date().toISOString()
          })
          .eq('id', productId);

        if (updateError) throw updateError;

        return new Response(
          JSON.stringify({
            success: true,
            message: `Moved ${quantity} units from warehouse to shelf`,
            data: {
              warehouse_stock: newWarehouseStock,
              shelf_stock: newShelfStock
            }
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      case 'adjust_shelf_stock': {
        // Adjust shelf stock (can be positive or negative)
        const { data: product, error: fetchError } = await supabase
          .from('products')
          .select('shelf_stock, warehouse_stock')
          .eq('id', productId)
          .single();

        if (fetchError || !product) {
          return new Response(
            JSON.stringify({ success: false, error: 'Product not found' }),
            { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }

        const newShelfStock = (product.shelf_stock || 0) + quantity;

        if (newShelfStock < 0) {
          return new Response(
            JSON.stringify({
              success: false,
              error: `Insufficient shelf stock. Available: ${product.shelf_stock || 0}, Requested: ${Math.abs(quantity)}`
            }),
            { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }

        const { error: updateError } = await supabase
          .from('products')
          .update({
            shelf_stock: newShelfStock,
            updated_at: new Date().toISOString()
          })
          .eq('id', productId);

        if (updateError) throw updateError;

        return new Response(
          JSON.stringify({
            success: true,
            message: `Adjusted shelf stock by ${quantity}. New shelf stock: ${newShelfStock}`,
            data: { shelf_stock: newShelfStock }
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      case 'get_stock_status': {
        // Get current stock levels
        const { data: product, error: fetchError } = await supabase
          .from('products')
          .select('warehouse_stock, shelf_stock, low_stock_threshold')
          .eq('id', productId)
          .single();

        if (fetchError || !product) {
          return new Response(
            JSON.stringify({ success: false, error: 'Product not found' }),
            { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }

        return new Response(
          JSON.stringify({
            success: true,
            data: {
              warehouse_stock: product.warehouse_stock || 0,
              shelf_stock: product.shelf_stock || 0,
              low_stock_threshold: product.low_stock_threshold || 0,
              is_low_stock: (product.shelf_stock || 0) <= (product.low_stock_threshold || 0)
            }
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      default:
        return new Response(
          JSON.stringify({ success: false, error: 'Invalid operation' }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
    }
  } catch (error) {
    console.error('Error in stock-management:', error);
    const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
    return new Response(
      JSON.stringify({ success: false, error: errorMessage }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
