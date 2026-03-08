-- ============================================
-- CONSOLIDATED MIGRATION - Complete Database Setup
-- ============================================
-- This migration consolidates all previous migrations into a single file
-- for easier reference and deployment
-- Created: 2026-02-12
-- ============================================

-- ============================================
-- SECTION 1: Gmail Secrets Configuration
-- ============================================
-- Remove unused backdoor secrets and add Gmail secrets for email functionality
-- Note: This section is skipped in local development due to vault permissions

DO $$
BEGIN
    -- Check if vault schema exists (only in production)
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'vault') THEN
        -- Remove backdoor secrets if they exist
        DELETE FROM vault.secrets WHERE name IN ('BACKDOOR_USERNAME', 'BACKDOOR_PASSWORD', 'BACKDOOR_ENABLED');
        
        -- Add Gmail secrets for email functionality
        INSERT INTO vault.secrets (name, secret, description)
        VALUES 
            ('GMAIL_CLIENT_ID', 'dummy-gmail-client-id', 'Gmail OAuth2 Client ID for sending emails')
        ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description;

        INSERT INTO vault.secrets (name, secret, description)
        VALUES 
            ('GMAIL_CLIENT_SECRET', 'dummy-gmail-client-secret', 'Gmail OAuth2 Client Secret for sending emails')
        ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description;

        INSERT INTO vault.secrets (name, secret, description)
        VALUES 
            ('GMAIL_USER', 'dummy-gmail-user@example.com', 'Gmail user email address for sending emails')
        ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description;

        INSERT INTO vault.secrets (name, secret, description)
        VALUES 
            ('GMAIL_API_KEY', 'dummy-gmail-api-key', 'Gmail API Key (if needed alongside OAuth2)')
        ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description;

        INSERT INTO vault.secrets (name, secret, description)
        VALUES 
            ('GMAIL_REFRESH_TOKEN', 'dummy-gmail-refresh-token', 'Gmail OAuth2 Refresh Token for sending emails')
        ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description;
        
        RAISE NOTICE 'Gmail secrets configured successfully';
    ELSE
        RAISE NOTICE 'Vault schema not found - skipping secrets configuration (local development)';
    END IF;
EXCEPTION WHEN OTHERS THEN
    -- Ignore errors if vault doesn't exist or permissions are insufficient
    RAISE NOTICE 'Skipping vault secrets configuration: %', SQLERRM;
END $$;

-- ============================================
-- SECTION 2: RLS Policies for Gamification & Stock Operations
-- ============================================

-- Drop existing policies that conflict
DROP POLICY IF EXISTS "Admins can manage n8n webhooks" ON public.n8n_webhooks;
DROP POLICY IF EXISTS "Users can view their own gamification logs" ON public.gamification_event_logs;
DROP POLICY IF EXISTS "Admins can view all gamification logs" ON public.gamification_event_logs;
DROP POLICY IF EXISTS "Anyone can view gamification rules" ON public.gamification_rules;
DROP POLICY IF EXISTS "Admins can manage gamification rules" ON public.gamification_rules;
DROP POLICY IF EXISTS "Users can view their own rule cooldowns" ON public.rule_cooldowns;
DROP POLICY IF EXISTS "System can manage rule cooldowns" ON public.rule_cooldowns;
DROP POLICY IF EXISTS "Admins can manage daily stock operations" ON public.daily_stock_operations;
DROP POLICY IF EXISTS "Admins can manage integration settings" ON public.integration_settings;

-- Enable RLS on tables
ALTER TABLE public.n8n_webhooks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gamification_event_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gamification_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.rule_cooldowns ENABLE ROW LEVEL SECURITY;

-- Create RLS policies (will be updated later to use has_app_role)
CREATE POLICY "Admins can manage n8n webhooks" 
ON public.n8n_webhooks 
FOR ALL 
USING (get_current_user_role() = ANY (ARRAY['admin'::user_role, 'developer'::user_role]));

CREATE POLICY "Users can view their own gamification logs" 
ON public.gamification_event_logs 
FOR SELECT 
USING (auth.uid() = user_id);

CREATE POLICY "Admins can view all gamification logs" 
ON public.gamification_event_logs 
FOR SELECT 
USING (get_current_user_role() = ANY (ARRAY['admin'::user_role, 'developer'::user_role]));

-- Note: gamification_rules policy will be set in Section 6 for authenticated users only

CREATE POLICY "Admins can manage gamification rules" 
ON public.gamification_rules 
FOR ALL 
USING (get_current_user_role() = ANY (ARRAY['admin'::user_role, 'developer'::user_role]));

CREATE POLICY "Users can view their own rule cooldowns" 
ON public.rule_cooldowns 
FOR SELECT 
USING (auth.uid() = user_id);

CREATE POLICY "System can manage rule cooldowns" 
ON public.rule_cooldowns 
FOR ALL 
USING (get_current_user_role() = ANY (ARRAY['admin'::user_role, 'developer'::user_role]));

CREATE POLICY "Admins can manage daily stock operations" 
ON public.daily_stock_operations 
FOR ALL 
USING (get_current_user_role() = ANY (ARRAY['admin'::user_role, 'developer'::user_role]));

CREATE POLICY "Admins can manage integration settings" 
ON public.integration_settings 
FOR ALL 
USING (get_current_user_role() = ANY (ARRAY['admin'::user_role, 'developer'::user_role]));

-- Add warehouse_stock column to daily_stock_operations
ALTER TABLE public.daily_stock_operations 
ADD COLUMN IF NOT EXISTS warehouse_stock integer DEFAULT 0;

-- ============================================
-- SECTION 3: Friendly Order IDs
-- ============================================

-- Create sequence for friendly order IDs (if not exists)
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = 'orders_friendly_id_seq') THEN
        CREATE SEQUENCE public.orders_friendly_id_seq START WITH 1;
    END IF;
END $$;

-- Generate user-friendly order IDs by creating a function and trigger
CREATE OR REPLACE FUNCTION generate_friendly_order_id()
RETURNS TEXT AS $$
DECLARE
    new_id TEXT;
    prefix TEXT := 'ORD';
    counter INTEGER;
BEGIN
    -- Get the current count of orders + 1
    SELECT COUNT(*) + 1 INTO counter FROM orders;
    
    -- Format as ORD0001, ORD0002, etc.
    new_id := prefix || LPAD(counter::TEXT, 4, '0');
    
    -- Ensure uniqueness (in case of concurrent inserts)
    WHILE EXISTS (SELECT 1 FROM orders WHERE friendly_id = new_id) LOOP
        counter := counter + 1;
        new_id := prefix || LPAD(counter::TEXT, 4, '0');
    END LOOP;
    
    RETURN new_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- SECTION 4: Daily Stock Snapshot + Remove Product Opening Stock
-- ============================================

-- Remove unused opening_stock from products (use daily_stock_operations opening_stock instead)
ALTER TABLE public.products
DROP COLUMN IF EXISTS opening_stock;

-- Create a helper to seed daily stock operations for a date
CREATE OR REPLACE FUNCTION public.seed_daily_stock_operations(p_date date DEFAULT current_date)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO public.daily_stock_operations (
    product_id,
    opening_stock,
    additional_stock,
    actual_closing_stock,
    estimated_closing_stock,
    stolen_stock,
    wastage_stock,
    sales,
    order_count,
    warehouse_stock,
    created_at
  )
  SELECT
    p.id,
    COALESCE(p.shelf_stock, 0),
    0,
    COALESCE(p.shelf_stock, 0),
    COALESCE(p.shelf_stock, 0),
    0,
    0,
    0,
    0,
    COALESCE(p.warehouse_stock, 0),
    p_date
  FROM public.products p
  WHERE COALESCE(p.is_archived, false) = false
    AND COALESCE(p.status, 'active') = 'active'
    AND NOT EXISTS (
      SELECT 1
      FROM public.daily_stock_operations d
      WHERE d.product_id = p.id
        AND d.created_at = p_date
    );
END;
$$;

-- Update additional_stock when shelf_stock increases
CREATE OR REPLACE FUNCTION public.track_additional_stock_on_shelf_increase()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_delta integer;
BEGIN
  IF NEW.shelf_stock IS NULL OR OLD.shelf_stock IS NULL THEN
    RETURN NEW;
  END IF;

  v_delta := NEW.shelf_stock - OLD.shelf_stock;
  IF v_delta <= 0 THEN
    RETURN NEW;
  END IF;

  -- Ensure today's row exists
  PERFORM public.seed_daily_stock_operations(current_date);

  UPDATE public.daily_stock_operations
  SET
    additional_stock = COALESCE(additional_stock, 0) + v_delta,
    estimated_closing_stock = opening_stock + (COALESCE(additional_stock, 0) + v_delta) - COALESCE(order_count, 0)
  WHERE product_id = NEW.id
    AND created_at = current_date;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS products_track_additional_stock ON public.products;
CREATE TRIGGER products_track_additional_stock
AFTER UPDATE OF shelf_stock ON public.products
FOR EACH ROW
EXECUTE FUNCTION public.track_additional_stock_on_shelf_increase();

-- Ensure new products get a daily stock row for today
CREATE OR REPLACE FUNCTION public.create_daily_stock_operation_on_product_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM public.daily_stock_operations d
    WHERE d.product_id = NEW.id
      AND d.created_at = current_date
  ) THEN
    INSERT INTO public.daily_stock_operations (
      product_id,
      opening_stock,
      additional_stock,
      actual_closing_stock,
      estimated_closing_stock,
      stolen_stock,
      wastage_stock,
      sales,
      order_count,
      warehouse_stock,
      created_at
    ) VALUES (
      NEW.id,
      COALESCE(NEW.shelf_stock, 0),
      0,
      COALESCE(NEW.shelf_stock, 0),
      COALESCE(NEW.shelf_stock, 0),
      0,
      0,
      0,
      0,
      COALESCE(NEW.warehouse_stock, 0),
      current_date
    );
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS products_insert_daily_stock ON public.products;
CREATE TRIGGER products_insert_daily_stock
AFTER INSERT ON public.products
FOR EACH ROW
EXECUTE FUNCTION public.create_daily_stock_operation_on_product_insert();

-- Schedule daily snapshot at 12:00 AM (if pg_cron is available)
DO $$
DECLARE
  v_jobid integer;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    SELECT jobid INTO v_jobid
    FROM cron.job
    WHERE jobname = 'daily_stock_snapshot';

    IF v_jobid IS NOT NULL THEN
      PERFORM cron.unschedule(v_jobid);
    END IF;

    PERFORM cron.schedule(
      'daily_stock_snapshot',
      '0 0 * * *',
      $$SELECT public.seed_daily_stock_operations(current_date);$$
    );
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Skipping pg_cron schedule: %', SQLERRM;
END;
$$;

-- Add friendly_id column to orders table
ALTER TABLE orders ADD COLUMN IF NOT EXISTS friendly_id TEXT UNIQUE;

-- Create trigger to auto-generate friendly IDs
CREATE OR REPLACE FUNCTION set_friendly_order_id()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.friendly_id IS NULL THEN
        NEW.friendly_id := generate_friendly_order_id();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS trigger_set_friendly_order_id ON orders;
CREATE TRIGGER trigger_set_friendly_order_id
    BEFORE INSERT ON orders
    FOR EACH ROW
    EXECUTE FUNCTION set_friendly_order_id();

-- Update existing orders with friendly IDs using a temporary sequence
DO $$
DECLARE
    rec RECORD;
    counter INTEGER := 1;
BEGIN
    FOR rec IN SELECT id FROM orders WHERE friendly_id IS NULL ORDER BY created_at LOOP
        UPDATE orders 
        SET friendly_id = 'ORD' || LPAD(counter::TEXT, 4, '0')
        WHERE id = rec.id;
        counter := counter + 1;
    END LOOP;
END $$;

-- ============================================
-- SECTION 4: Product Management Policies
-- ============================================

-- Ensure RLS is enabled on products table
ALTER TABLE public.products ENABLE ROW LEVEL SECURITY;

-- Drop conflicting policies
DROP POLICY IF EXISTS "Admins can update products" ON public.products;
DROP POLICY IF EXISTS "Admins can manage products" ON public.products;

-- Create unified admin management policy using has_role
CREATE POLICY "Admins can manage products"
ON public.products
FOR ALL
TO public
USING (
  public.has_role(auth.uid(), 'admin') 
  OR public.has_role(auth.uid(), 'developer')
)
WITH CHECK (
  public.has_role(auth.uid(), 'admin') 
  OR public.has_role(auth.uid(), 'developer')
);

-- Create or replace function to update updated_at timestamp
CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- Ensure updated_at is maintained automatically on product updates
DROP TRIGGER IF EXISTS update_products_updated_at ON public.products;
CREATE TRIGGER update_products_updated_at
BEFORE UPDATE ON public.products
FOR EACH ROW
EXECUTE FUNCTION public.update_updated_at_column();

-- ============================================
-- SECTION 5: Stock Management Functions Cleanup
-- ============================================

-- Drop order-based stock change function (if exists)
DROP FUNCTION IF EXISTS public.apply_order_stock_change(p_order_id uuid, p_action text, p_actor_user_id uuid);

-- Drop core stock adjustment function (if exists)
DROP FUNCTION IF EXISTS public.adjust_product_stock(p_product_id uuid, p_delta_shelf integer, p_delta_warehouse integer, p_reason text, p_order_id uuid, p_actor_user_id uuid, p_adjust_opening boolean);

-- ============================================
-- SECTION 6: Security Restrictions - Authenticated Access Only
-- ============================================

-- Restrict points_config to authenticated users only
DROP POLICY IF EXISTS "Anyone can view points config" ON public.points_config;
CREATE POLICY "Authenticated users can view points config"
ON public.points_config
FOR SELECT
TO authenticated
USING (auth.uid() IS NOT NULL);

-- Restrict badges to authenticated users only
DROP POLICY IF EXISTS "Anyone can view badges" ON public.badges;
CREATE POLICY "Authenticated users can view badges"
ON public.badges
FOR SELECT
TO authenticated
USING (auth.uid() IS NOT NULL);

-- Restrict gamification_rules to authenticated users only
DROP POLICY IF EXISTS "Anyone can view gamification rules" ON public.gamification_rules;
CREATE POLICY "Authenticated users can view gamification rules"
ON public.gamification_rules
FOR SELECT
TO authenticated
USING (auth.uid() IS NOT NULL);

-- Restrict college_settings to authenticated users only
DROP POLICY IF EXISTS "Anyone can view college settings" ON public.college_settings;
CREATE POLICY "Authenticated users can view college settings"
ON public.college_settings
FOR SELECT
TO authenticated
USING (auth.uid() IS NOT NULL);

-- Restrict top_students leaderboard to authenticated users only
DROP POLICY IF EXISTS "Allow public read access to active top students" ON public.top_students;
CREATE POLICY "Authenticated users can view active top students"
ON public.top_students
FOR SELECT
TO authenticated
USING (is_archived = false AND auth.uid() IS NOT NULL);

-- Restrict top_departments leaderboard to authenticated users only
DROP POLICY IF EXISTS "Allow public read access to active top departments" ON public.top_departments;
CREATE POLICY "Authenticated users can view active top departments"
ON public.top_departments
FOR SELECT
TO authenticated
USING (is_archived = false AND auth.uid() IS NOT NULL);

-- ============================================
-- SECTION 7: User Profile Security
-- ============================================

-- Drop dangerous update policies
DROP POLICY IF EXISTS "Users can update their own orders" ON public.orders;
DROP POLICY IF EXISTS "Users can update own profile" ON public.users;

-- Create restrictive order update policy (will be modified later)
CREATE POLICY "Users cannot update orders after creation" ON public.orders
  FOR UPDATE
  TO authenticated
  USING (false);

-- Create function to validate user profile updates
CREATE OR REPLACE FUNCTION validate_user_profile_update()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  -- Allow admins to update anything
  IF get_current_user_role() IN ('admin', 'developer') THEN
    RETURN NEW;
  END IF;
  
  -- For regular users, prevent changing critical fields
  IF auth.uid() = NEW.id THEN
    -- Restore protected fields to their original values
    NEW.role := OLD.role;
    NEW.points := OLD.points;
    NEW.status := OLD.status;
    NEW.student_id := OLD.student_id;
    NEW.name := OLD.name;
    NEW.department := OLD.department;
    NEW.shift := OLD.shift;
    NEW.password_changed_at := OLD.password_changed_at;
    NEW.last_signed_in_at := OLD.last_signed_in_at;
    NEW.created_at := OLD.created_at;
    
    -- Users can only update: mobile_number, email
    RETURN NEW;
  END IF;
  
  -- If not admin and not own profile, reject
  RAISE EXCEPTION 'Unauthorized update attempt';
END;
$$;

-- Create trigger for user profile validation
DROP TRIGGER IF EXISTS validate_user_update ON public.users;
CREATE TRIGGER validate_user_update
  BEFORE UPDATE ON public.users
  FOR EACH ROW
  EXECUTE FUNCTION validate_user_profile_update();

-- Create basic update policy for users
CREATE POLICY "Users can update own safe profile fields" ON public.users
  FOR UPDATE
  TO authenticated
  USING (auth.uid() = id);

-- Add documentation comments
COMMENT ON POLICY "Users cannot update orders after creation" ON public.orders IS 
  'Security: Prevents users from manipulating payment status, amount, or transaction details.';

COMMENT ON POLICY "Users can update own safe profile fields" ON public.users IS 
  'Security: Users can update their profile but trigger validates only safe fields (mobile_number, email).';

COMMENT ON FUNCTION validate_user_profile_update() IS 
  'Security trigger: Ensures users cannot modify critical fields (role, points, status, etc).';

-- ============================================
-- SECTION 8: Admin Audit Logging
-- ============================================

-- Create admin audit log table
CREATE TABLE IF NOT EXISTS public.admin_audit_log (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  user_role user_role,
  action text NOT NULL,
  table_name text NOT NULL,
  record_id uuid,
  old_values jsonb,
  new_values jsonb,
  ip_address text,
  user_agent text,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_admin_audit_log_user_id ON public.admin_audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_admin_audit_log_table_name ON public.admin_audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_admin_audit_log_created_at ON public.admin_audit_log(created_at DESC);

-- Enable RLS
ALTER TABLE public.admin_audit_log ENABLE ROW LEVEL SECURITY;

-- Admin-only view policy
CREATE POLICY "Admins can view all audit logs"
ON public.admin_audit_log
FOR SELECT
USING (get_current_user_role() IN ('admin', 'developer'));

-- Create trigger function for audit logging
CREATE OR REPLACE FUNCTION public.log_admin_action()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  current_user_role user_role;
  audit_record_id uuid;
BEGIN
  -- Get the current user's role
  SELECT role INTO current_user_role
  FROM public.users
  WHERE id = auth.uid();
  
  -- Only log if user is admin or developer
  IF current_user_role NOT IN ('admin', 'developer') THEN
    RETURN COALESCE(NEW, OLD);
  END IF;
  
  -- Determine the record ID
  IF TG_OP = 'DELETE' THEN
    audit_record_id := OLD.id;
  ELSE
    audit_record_id := NEW.id;
  END IF;
  
  -- Insert audit log entry
  INSERT INTO public.admin_audit_log (
    user_id, user_role, action, table_name, record_id,
    old_values, new_values, created_at
  ) VALUES (
    COALESCE(auth.uid(), '00000000-0000-0000-0000-000000000000'::uuid),
    current_user_role, TG_OP, TG_TABLE_NAME, audit_record_id,
    CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN to_jsonb(OLD) ELSE NULL END,
    CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN to_jsonb(NEW) ELSE NULL END,
    now()
  );
  
  RETURN COALESCE(NEW, OLD);
END;
$$;

-- Create audit triggers for users table
DROP TRIGGER IF EXISTS audit_users_insert ON public.users;
CREATE TRIGGER audit_users_insert
AFTER INSERT ON public.users
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

DROP TRIGGER IF EXISTS audit_users_update ON public.users;
CREATE TRIGGER audit_users_update
AFTER UPDATE ON public.users
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

DROP TRIGGER IF EXISTS audit_users_delete ON public.users;
CREATE TRIGGER audit_users_delete
AFTER DELETE ON public.users
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

-- Create audit triggers for orders table
DROP TRIGGER IF EXISTS audit_orders_insert ON public.orders;
CREATE TRIGGER audit_orders_insert
AFTER INSERT ON public.orders
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

DROP TRIGGER IF EXISTS audit_orders_update ON public.orders;
CREATE TRIGGER audit_orders_update
AFTER UPDATE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

DROP TRIGGER IF EXISTS audit_orders_delete ON public.orders;
CREATE TRIGGER audit_orders_delete
AFTER DELETE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

-- Create audit triggers for integration_settings table
DROP TRIGGER IF EXISTS audit_integration_settings_insert ON public.integration_settings;
CREATE TRIGGER audit_integration_settings_insert
AFTER INSERT ON public.integration_settings
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

DROP TRIGGER IF EXISTS audit_integration_settings_update ON public.integration_settings;
CREATE TRIGGER audit_integration_settings_update
AFTER UPDATE ON public.integration_settings
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

DROP TRIGGER IF EXISTS audit_integration_settings_delete ON public.integration_settings;
CREATE TRIGGER audit_integration_settings_delete
AFTER DELETE ON public.integration_settings
FOR EACH ROW
EXECUTE FUNCTION public.log_admin_action();

-- Add documentation
COMMENT ON TABLE public.admin_audit_log IS 'Comprehensive audit log tracking all admin actions on sensitive tables';
COMMENT ON FUNCTION public.log_admin_action() IS 'Trigger function to automatically log admin actions with timestamps and changed values';

-- ============================================
-- SECTION 9: Order Payment Update Policy
-- ============================================

-- Drop the restrictive policy and allow users to update payment on unpaid orders
DROP POLICY IF EXISTS "Users cannot update orders after creation" ON public.orders;

CREATE POLICY "Users can update payment on their unpaid orders" 
ON public.orders 
FOR UPDATE 
USING (auth.uid() = user_id AND payment_status = 'unpaid')
WITH CHECK (auth.uid() = user_id);

-- ============================================
-- SECTION 10: Update Profile Validation Function
-- ============================================

-- Update the validate_user_profile_update function to allow admins to update role
CREATE OR REPLACE FUNCTION public.validate_user_profile_update()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $$
BEGIN
  -- Allow admins to update anything
  IF get_current_user_role() IN ('admin', 'developer') THEN
    RETURN NEW;
  END IF;
  
  -- For regular users, prevent changing critical fields
  IF auth.uid() = NEW.id THEN
    -- Restore protected fields to their original values
    NEW.role := OLD.role;
    NEW.points := OLD.points;
    NEW.status := OLD.status;
    NEW.student_id := OLD.student_id;
    NEW.name := OLD.name;
    NEW.department := OLD.department;
    NEW.shift := OLD.shift;
    NEW.password_changed_at := OLD.password_changed_at;
    NEW.last_signed_in_at := OLD.last_signed_in_at;
    NEW.created_at := OLD.created_at;
    
    -- Users can only update: mobile_number, email
    RETURN NEW;
  END IF;
  
  -- If not admin and not own profile, reject
  RAISE EXCEPTION 'Unauthorized update attempt';
END;
$$;

-- ============================================
-- SECTION 11: App Role Management System
-- ============================================

-- Create the app_role enum type
DO $$ BEGIN
    CREATE TYPE public.app_role AS ENUM ('admin', 'student', 'teacher', 'developer');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create user_roles table
CREATE TABLE IF NOT EXISTS public.user_roles (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid REFERENCES auth.users(id) ON DELETE CASCADE NOT NULL,
    role app_role NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    UNIQUE (user_id, role)
);

-- Enable Row-Level Security
ALTER TABLE public.user_roles ENABLE ROW LEVEL SECURITY;

-- Create security definer function to check roles (prevents RLS recursion)
CREATE OR REPLACE FUNCTION public.has_app_role(_user_id uuid, _role app_role)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.user_roles
    WHERE user_id = _user_id
      AND role = _role
  )
$$;

-- Create function to get current user's app role
CREATE OR REPLACE FUNCTION public.get_current_app_role()
RETURNS app_role
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT role FROM public.user_roles WHERE user_id = auth.uid() LIMIT 1;
$$;

-- RLS policies for user_roles table
DROP POLICY IF EXISTS "Admins can view all user roles" ON public.user_roles;
CREATE POLICY "Admins can view all user roles"
ON public.user_roles
FOR SELECT
TO authenticated
USING (public.has_app_role(auth.uid(), 'admin') OR public.has_app_role(auth.uid(), 'developer'));

DROP POLICY IF EXISTS "Admins can insert user roles" ON public.user_roles;
CREATE POLICY "Admins can insert user roles"
ON public.user_roles
FOR INSERT
TO authenticated
WITH CHECK (public.has_app_role(auth.uid(), 'admin') OR public.has_app_role(auth.uid(), 'developer'));

DROP POLICY IF EXISTS "Admins can update user roles" ON public.user_roles;
CREATE POLICY "Admins can update user roles"
ON public.user_roles
FOR UPDATE
TO authenticated
USING (public.has_app_role(auth.uid(), 'admin') OR public.has_app_role(auth.uid(), 'developer'));

DROP POLICY IF EXISTS "Admins can delete user roles" ON public.user_roles;
CREATE POLICY "Admins can delete user roles"
ON public.user_roles
FOR DELETE
TO authenticated
USING (public.has_app_role(auth.uid(), 'admin') OR public.has_app_role(auth.uid(), 'developer'));

DROP POLICY IF EXISTS "Users can view their own role" ON public.user_roles;
CREATE POLICY "Users can view their own role"
ON public.user_roles
FOR SELECT
TO authenticated
USING (user_id = auth.uid());

-- Migrate existing roles from users table to user_roles table
INSERT INTO public.user_roles (user_id, role)
SELECT id, role::text::app_role 
FROM public.users 
WHERE role IS NOT NULL
ON CONFLICT (user_id, role) DO NOTHING;

-- Create trigger to automatically create user_role when new user is created
CREATE OR REPLACE FUNCTION public.handle_new_user_role()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  INSERT INTO public.user_roles (user_id, role)
  VALUES (NEW.id, COALESCE(NEW.role::text::app_role, 'student'::app_role))
  ON CONFLICT (user_id, role) DO NOTHING;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS on_user_created_add_role ON public.users;
CREATE TRIGGER on_user_created_add_role
  AFTER INSERT ON public.users
  FOR EACH ROW
  EXECUTE FUNCTION public.handle_new_user_role();

-- Create trigger to sync role changes from users table to user_roles
CREATE OR REPLACE FUNCTION public.sync_user_role_change()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  -- Delete old role if it exists
  DELETE FROM public.user_roles WHERE user_id = NEW.id;
  -- Insert new role
  INSERT INTO public.user_roles (user_id, role)
  VALUES (NEW.id, NEW.role::text::app_role)
  ON CONFLICT (user_id, role) DO NOTHING;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS on_user_role_updated ON public.users;
CREATE TRIGGER on_user_role_updated
  AFTER UPDATE OF role ON public.users
  FOR EACH ROW
  WHEN (OLD.role IS DISTINCT FROM NEW.role)
  EXECUTE FUNCTION public.sync_user_role_change();

-- ============================================
-- SECTION 12: Update RLS Policies to use has_app_role()
-- ============================================

-- USERS TABLE - Update admin policies
DROP POLICY IF EXISTS "Admins view all profiles" ON public.users;
CREATE POLICY "Admins view all profiles" ON public.users
  FOR SELECT USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

DROP POLICY IF EXISTS "Admins update all profiles" ON public.users;
CREATE POLICY "Admins update all profiles" ON public.users
  FOR UPDATE USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

DROP POLICY IF EXISTS "Admins delete profiles" ON public.users;
CREATE POLICY "Admins delete profiles" ON public.users
  FOR DELETE USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

DROP POLICY IF EXISTS "Admins insert profiles" ON public.users;
CREATE POLICY "Admins insert profiles" ON public.users
  FOR INSERT WITH CHECK (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

-- ADMIN_AUDIT_LOG TABLE
DROP POLICY IF EXISTS "Admins can view all audit logs" ON public.admin_audit_log;
CREATE POLICY "Admins can view all audit logs" ON public.admin_audit_log
  FOR SELECT USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

-- DAILY_STOCK_OPERATIONS TABLE
DROP POLICY IF EXISTS "Admins can manage daily stock operations" ON public.daily_stock_operations;
CREATE POLICY "Admins can manage daily stock operations" ON public.daily_stock_operations
  FOR ALL USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

-- GAMIFICATION_EVENT_LOGS TABLE
DROP POLICY IF EXISTS "Admins can view all gamification logs" ON public.gamification_event_logs;
CREATE POLICY "Admins can view all gamification logs" ON public.gamification_event_logs
  FOR SELECT USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

-- GAMIFICATION_RULES TABLE
DROP POLICY IF EXISTS "Admins can manage gamification rules" ON public.gamification_rules;
CREATE POLICY "Admins can manage gamification rules" ON public.gamification_rules
  FOR ALL USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

-- INTEGRATION_SETTINGS TABLE
DROP POLICY IF EXISTS "Admins can manage integration settings" ON public.integration_settings;
CREATE POLICY "Admins can manage integration settings" ON public.integration_settings
  FOR ALL USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

-- N8N_WEBHOOKS TABLE
DROP POLICY IF EXISTS "Admins can manage n8n webhooks" ON public.n8n_webhooks;
CREATE POLICY "Admins can manage n8n webhooks" ON public.n8n_webhooks
  FOR ALL USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

-- RULE_COOLDOWNS TABLE
DROP POLICY IF EXISTS "System can manage rule cooldowns" ON public.rule_cooldowns;
CREATE POLICY "System can manage rule cooldowns" ON public.rule_cooldowns
  FOR ALL USING (
    has_app_role(auth.uid(), 'admin') OR 
    has_app_role(auth.uid(), 'developer')
  );

-- ============================================
-- SECTION 13: Function Search Path Fixes
-- ============================================

-- Create extensions schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS extensions;
GRANT USAGE ON SCHEMA extensions TO postgres, anon, authenticated, service_role;

-- Update generate_friendly_order_id with proper search_path and sequence
CREATE OR REPLACE FUNCTION public.generate_friendly_order_id()
 RETURNS text
 LANGUAGE plpgsql
 SET search_path = public
AS $$
DECLARE
    new_id TEXT;
    prefix TEXT := 'ORD';
    counter BIGINT;
BEGIN
    -- Try to use sequence if it exists, otherwise use count
    BEGIN
        counter := nextval('orders_friendly_id_seq');
    EXCEPTION WHEN undefined_table THEN
        SELECT COUNT(*) + 1 INTO counter FROM orders;
    END;
    
    -- Format as ORD0001, ORD0002, etc.
    new_id := prefix || LPAD(counter::TEXT, 4, '0');
    
    RETURN new_id;
END;
$$;

-- Update set_friendly_order_id with proper search_path
CREATE OR REPLACE FUNCTION public.set_friendly_order_id()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path = public
AS $$
BEGIN
    IF NEW.friendly_id IS NULL THEN
        NEW.friendly_id := generate_friendly_order_id();
    END IF;
    RETURN NEW;
END;
$$;

-- Update trigger_rankings_update with proper search_path
CREATE OR REPLACE FUNCTION public.trigger_rankings_update()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path = public
AS $$
BEGIN
  -- Only update if points actually changed
  IF (TG_OP = 'UPDATE' AND OLD.points IS DISTINCT FROM NEW.points) OR TG_OP = 'INSERT' THEN
    PERFORM refresh_rankings();
  END IF;
  
  RETURN COALESCE(NEW, OLD);
END;
$$;

-- Update refresh_rankings with proper search_path
CREATE OR REPLACE FUNCTION public.refresh_rankings()
 RETURNS void
 LANGUAGE plpgsql
 SET search_path = public
AS $$
BEGIN
  -- Call both ranking update functions
  PERFORM public.update_top_students_rankings();
  PERFORM public.update_top_departments_rankings();
END;
$$;

-- Update update_top_students_rankings with proper search_path
CREATE OR REPLACE FUNCTION public.update_top_students_rankings()
 RETURNS void
 LANGUAGE plpgsql
 SET search_path = public
AS $$
BEGIN
  -- Archive existing active rankings
  UPDATE public.top_students 
  SET is_archived = TRUE, updated_at = NOW()
  WHERE is_archived = FALSE;
  
  -- Insert new top 10 students
  INSERT INTO public.top_students (student_id, name, department, points, rank, is_archived)
  SELECT 
    u.student_id,
    u.name,
    u.department,
    u.points,
    ROW_NUMBER() OVER (ORDER BY u.points DESC) as rank,
    FALSE
  FROM public.users u
  WHERE u.role = 'student' AND u.status = 'active' AND u.points > 0
  ORDER BY u.points DESC
  LIMIT 10;
END;
$$;

-- Update update_top_departments_rankings with proper search_path
CREATE OR REPLACE FUNCTION public.update_top_departments_rankings()
 RETURNS void
 LANGUAGE plpgsql
 SET search_path = public
AS $$
BEGIN
  -- Archive existing active rankings
  UPDATE public.top_departments 
  SET is_archived = TRUE, updated_at = NOW()
  WHERE is_archived = FALSE;
  
  -- Insert new top 3 departments
  INSERT INTO public.top_departments (department, points, rank, is_archived)
  SELECT 
    u.department,
    SUM(u.points) as total_points,
    ROW_NUMBER() OVER (ORDER BY SUM(u.points) DESC) as rank,
    FALSE
  FROM public.users u
  WHERE u.role = 'student' AND u.status = 'active' AND u.department IS NOT NULL AND u.points > 0
  GROUP BY u.department
  ORDER BY total_points DESC
  LIMIT 3;
END;
$$;

-- Fix search_path for update_user_mfa_updated_at function
CREATE OR REPLACE FUNCTION public.update_user_mfa_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path = public
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

-- ============================================
-- SECTION 14: Notification Read Policies
-- ============================================

-- Allow users to insert their own notification read records
DROP POLICY IF EXISTS "Users can insert own notification reads" ON public.notification_reads;
CREATE POLICY "Users can insert own notification reads"
  ON public.notification_reads FOR INSERT
  TO authenticated
  WITH CHECK (auth.uid() = user_id);

-- Allow users to update their own notification read records
DROP POLICY IF EXISTS "Users can update own notification reads" ON public.notification_reads;
CREATE POLICY "Users can update own notification reads"
  ON public.notification_reads FOR UPDATE
  TO authenticated
  USING (auth.uid() = user_id);

-- ============================================
-- MIGRATION COMPLETE
-- ============================================
-- This consolidated migration includes:
-- 1. Gmail secrets configuration
-- 2. RLS policies for gamification & stock operations
-- 3. Friendly order IDs
-- 4. Product management policies
-- 5. Stock management cleanup
-- 6. Security restrictions (authenticated-only access)
-- 7. User profile security
-- 8. Admin audit logging
-- 9. Order payment update policy
-- 10. Profile validation function updates
-- 11. App role management system
-- 12. RLS policies updated to use has_app_role()
-- 13. Function search_path fixes
-- 14. Notification read policies
-- ============================================
