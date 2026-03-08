import { useState, useEffect, useCallback, useRef } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useToast } from '@/hooks/use-toast';
import { WHITELABEL_CONFIG } from '@/config';
import { Loader2, Save, Users, Clock, AlertTriangle, Grid, List, Package } from 'lucide-react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { PRODUCT_CATEGORIES } from '@/constants/productCategories';
import { useAuth } from '@/contexts/useAuth';
import { AuditLogger } from '@/utils/auditLogger';
import { Badge } from '@/features/gamification/components/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { StockOperationCardGrid } from '@/components/stock/StockOperationCardGrid';
import { useResponsiveView } from '@/hooks/useResponsiveView';
import { LoadingSpinner } from '@/components/common/LoadingSpinner';

// Product as stored in DB
interface Product {
  id: string;
  name: string;
  description?: string;
  price?: number;
  category: string;
  shelf_stock: number;
  warehouse_stock: number;
  created_at?: string;
  created_by?: string;
  image_url?: string;
  is_archived?: boolean;
  status?: string;
  unit_price?: number;
  updated_by?: string;
  updated_at?: string;
  [key: string]: string | number | boolean | undefined;
}

// Stock operation as stored in DB (no product field)
interface StockOperationRow {
  id?: string;
  product_id: string;
  opening_stock: number;
  additional_stock: number;
  actual_closing_stock: number;
  estimated_closing_stock: number;
  stolen_stock: number;
  wastage_stock: number;
  warehouse_stock: number;
  sales: number;
  order_count: number;
  created_at: string;
  updated_at?: string | null;
}

// Stock operation as used in UI (with product field)
interface StockOperation extends StockOperationRow {
  product: Product;
}

interface Filters {
  category: string;
  status: string;
  stockStatus: string;
}

interface RealtimeUpdate {
  id: string;
  user_id: string;
  user_name: string;
  product_id: string;
  product_name: string;
  field: string;
  old_value: number;
  new_value: number;
  timestamp: string;
}

interface ActiveUser {
  user_id: string;
  user_name: string;
  last_activity: string;
  editing_product_id?: string;
}

const AdminStockAccounting = () => {
  const [products, setProducts] = useState<Product[]>([]);
  const [stockOperations, setStockOperations] = useState<StockOperation[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [filters, setFilters] = useState<Filters>({
    category: 'all',
    status: 'all',
    stockStatus: 'all',
  });
  const [viewMode, setViewMode] = useState<'table' | 'cards'>('table');
  const isMobile = useResponsiveView();

  // Real-time and audit trail states
  const [realtimeUpdates, setRealtimeUpdates] = useState<RealtimeUpdate[]>([]);
  const [activeUsers, setActiveUsers] = useState<ActiveUser[]>([]);
  const [conflictWarnings, setConflictWarnings] = useState<string[]>([]);
  const realtimeChannelRef = useRef<ReturnType<typeof supabase.channel> | null>(null);
  const auditLogger = useRef(AuditLogger.getInstance());
  const lastActivityRef = useRef<number>(Date.now());
  const stockOperationsRef = useRef<StockOperation[]>([]);

  const { toast } = useToast();
  const { user, profile } = useAuth();
  const errorMessages = WHITELABEL_CONFIG.messages.errors;
  const today = new Date().toISOString().split('T')[0];
  const getLocalDayRange = useCallback(() => {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const end = new Date(start);
    end.setDate(end.getDate() + 1);
    return { start, end };
  }, []);

  const computeEstimatedClosing = useCallback((opening: number, additional: number, orderCount: number) => {
    return (opening || 0) + (additional || 0) - (orderCount || 0);
  }, []);

  const computeStolenStock = useCallback((estimated: number, actual: number, wastage: number) => {
    return Math.max(0, (estimated || 0) - (actual || 0) - (wastage || 0));
  }, []);

  const computeSalesAmount = useCallback((unitPrice: number, orderCount: number) => {
    return (unitPrice || 0) * (orderCount || 0);
  }, []);

  const getValidationIssues = useCallback((op: StockOperation) => {
    const estimated = computeEstimatedClosing(op.opening_stock, op.additional_stock, op.order_count);
    const actual = op.actual_closing_stock || 0;
    const wastage = op.wastage_stock || 0;
    return {
      estimated,
      exceedsEstimated: actual > estimated,
      exceedsWithWastage: actual + wastage > estimated,
    };
  }, [computeEstimatedClosing]);

  // Auto-switch to cards view on mobile
  useEffect(() => {
    if (isMobile && viewMode === 'table') {
      setViewMode('cards');
    }
  }, [isMobile, viewMode]);

  const broadcastUserActivity = useCallback(async () => {
    if (!user || !profile) return;

    const activity: ActiveUser = {
      user_id: user.id,
      user_name: profile.name || 'Unknown User',
      last_activity: new Date().toISOString(),
    };

    // This would ideally be stored in a temporary table or Redis
    // For now, we'll use local state
    setActiveUsers(prev => {
      const filtered = prev.filter(u => u.user_id !== user.id);
      return [...filtered, activity];
    });
  }, [user, profile]);

  // Real-time setup and cleanup
  const setupRealtimeSubscription = useCallback(() => {
    if (!user) return;
    const { start } = getLocalDayRange();

    // Subscribe to stock operations changes
    const channel = supabase
      .channel('stock_operations_realtime')
      .on(
        'postgres_changes',
        {
          event: '*',
          schema: 'public',
          table: 'daily_stock_operations',
          filter: `created_at=gte.${start.toISOString()}`
        },
        async (payload) => {
          try {
            if (payload.eventType === 'UPDATE' && payload.new && payload.old) {
              const newData = payload.new as StockOperationRow;
              const oldData = payload.old as StockOperationRow;

              // Find which field changed
              const changedFields = Object.keys(newData).filter(key =>
                key !== 'updated_at' &&
                key !== 'created_at' &&
                newData[key as keyof StockOperationRow] !== oldData[key as keyof StockOperationRow]
              );

              if (changedFields.length > 0) {
                // Find product name from existing local state instead of API call
                const existingOperation = stockOperationsRef.current.find(op => op.product_id === newData.product_id);
                const productName = existingOperation?.product?.name || 'Unknown Product';

                // Skip if this is our own update (prevent self-notifications)
                if (newData.updated_at && Date.now() - new Date(newData.updated_at).getTime() < 2000) {
                  return; // Skip recent updates that might be from current user
                }

                const updateInfo: RealtimeUpdate = {
                  id: `${newData.id}-${Date.now()}`,
                  user_id: 'unknown', // Would need to track this
                  user_name: 'Another User',
                  product_id: newData.product_id,
                  product_name: productName,
                  field: changedFields[0],
                  old_value: oldData[changedFields[0] as keyof StockOperationRow] as number,
                  new_value: newData[changedFields[0] as keyof StockOperationRow] as number,
                  timestamp: new Date().toISOString()
                };

                setRealtimeUpdates(prev => [updateInfo, ...prev.slice(0, 9)]); // Keep last 10

                // Show conflict warning with proper cleanup
                setConflictWarnings(prev => [...prev, `${productName} was updated by another user`]);
                const timeoutId = setTimeout(() => {
                  setConflictWarnings(prev => prev.slice(1));
                }, 5000);
                conflictTimeoutsRef.current.push(timeoutId);

                // Update local state
                setStockOperations(prev =>
                  prev.map(op => op.id === newData.id ? { ...op, ...newData } : op)
                );
              }
            }
          } catch (error) {
            console.error('Error handling real-time update:', error);
          }
        }
      )
      .subscribe();

    realtimeChannelRef.current = channel;

    // Track user activity - only broadcast if actually active
    const activityInterval = setInterval(() => {
      const timeSinceLastActivity = Date.now() - lastActivityRef.current;
      if (timeSinceLastActivity < 30000) { // Active in last 30 seconds
        broadcastUserActivity();
      }
    }, 15000); // Reduced frequency: Broadcast every 15 seconds instead of 10

    return () => {
      clearInterval(activityInterval);
      if (realtimeChannelRef.current) {
        supabase.removeChannel(realtimeChannelRef.current);
      }
    };
  }, [user, broadcastUserActivity, getLocalDayRange]);

  const trackUserActivity = useCallback(() => {
    lastActivityRef.current = Date.now();
  }, []);

  const auditTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const conflictTimeoutsRef = useRef<NodeJS.Timeout[]>([]);

  const logAuditTrail = useCallback(async (
    action: string,
    productId: string,
    productName: string,
    field: string,
    oldValue: number,
    newValue: number
  ) => {
    if (!user || !auditLogger.current) return;

    // Debounce audit logging to prevent excessive logs during rapid input
    if (auditTimeoutRef.current) {
      clearTimeout(auditTimeoutRef.current);
    }

    auditTimeoutRef.current = setTimeout(async () => {
      await auditLogger.current.log(
        `stock_${action}`,
        'daily_stock_operations',
        {
          product_id: productId,
          product_name: productName,
          field,
          old_value: oldValue,
          new_value: newValue,
          user_id: user.id,
          user_name: profile?.name || 'Unknown User',
          timestamp: new Date().toISOString(),
          date: today
        },
        'medium'
      );
    }, 3000); // Wait 3 seconds after last change before logging
  }, [user, profile, today]);

  const loadStockOperations = useCallback(async () => {
    setLoading(true);
    try {
      // Load products
      const { data: productsData, error: productsError } = await supabase
        .from('products')
        .select('*')
        .order('name');

      if (productsError) throw productsError;

      // Format today's date to match the database format
      const todayFormatted = new Date().toISOString().split('T')[0];

      // Build local date range for today
      const { start: todayStartDate, end: tomorrowStartDate } = getLocalDayRange();
      const todayStart = todayStartDate.toISOString();
      const tomorrowStart = tomorrowStartDate.toISOString();

      // Load today's operations
      const { data: operationsData, error: operationsError } = await supabase
        .from('daily_stock_operations')
        .select('*')
        .gte('created_at', todayStart)
        .lt('created_at', tomorrowStart);

      if (operationsError) throw operationsError;

      // Load today's sold quantities per product (sum of order_items.quantity for today)
      const { data: orderItemsData, error: orderItemsError } = await supabase
        .from('order_items')
        .select('product_id, quantity, orders!inner(created_at, payment_status)')
        .gte('orders.created_at', todayStart)
        .lt('orders.created_at', tomorrowStart)
        .neq('orders.payment_status', 'cancelled');

      if (orderItemsError) throw orderItemsError;

      const todaySoldQtyMap = new Map<string, number>();
      (orderItemsData ?? []).forEach((item: { product_id: string; quantity: number }) => {
        if (!item.product_id) return;
        todaySoldQtyMap.set(
          item.product_id,
          (todaySoldQtyMap.get(item.product_id) || 0) + (item.quantity || 0)
        );
      });

      // Load latest previous operations to get closing stock (search backwards until found)
      const { data: previousOperationsData, error: previousError } = await supabase
        .from('daily_stock_operations')
        .select('product_id, actual_closing_stock, created_at')
        .lt('created_at', todayStart)
        .order('created_at', { ascending: false });

      if (previousError) throw previousError;

      // Create a map of latest previous closing stock by product_id
      const previousClosingStockMap = new Map<string, number>();
      (previousOperationsData ?? []).forEach(op => {
        if (!op.product_id || op.actual_closing_stock === null) return;
        if (!previousClosingStockMap.has(op.product_id)) {
          previousClosingStockMap.set(op.product_id, op.actual_closing_stock);
        }
      });

      // Type assertion for operationsData with proper handling
      const opsData: StockOperationRow[] = (operationsData ?? []).map((op: Omit<StockOperationRow, 'updated_at' | 'warehouse_stock'> & {
        updated_at?: string | null;
        warehouse_stock?: number;
      }) => ({
        ...op,
        updated_at: op.updated_at || null,
        warehouse_stock: op.warehouse_stock || 0
      }));

      // Transform products and merge with operations
      const transformedProducts = (productsData ?? []).map(item => ({
        ...item,
        shelf_stock: item.shelf_stock || 0,
        warehouse_stock: item.warehouse_stock || 0,
      }));


      // Handle multiple operations per product per day
      const mergedOperations: StockOperation[] = [];

      // First, add all existing operations for today
      opsData.forEach(op => {
        const product = transformedProducts.find(p => p.id === op.product_id);
        if (product) {
          const orderQty = todaySoldQtyMap.get(op.product_id) ?? 0;
          const estimatedClosing = computeEstimatedClosing(
            op.opening_stock || 0,
            op.additional_stock || 0,
            orderQty
          );
          const stolenStock = computeStolenStock(
            estimatedClosing,
            op.actual_closing_stock || 0,
            op.wastage_stock || 0
          );
          const unitPrice = product.unit_price || 0;
          const salesAmount = computeSalesAmount(unitPrice, orderQty);
          mergedOperations.push({
            ...op,
            order_count: orderQty,
            estimated_closing_stock: estimatedClosing,
            stolen_stock: stolenStock,
            sales: salesAmount,
            product,
          });
        }
      });

      // Then, add products that don't have any operations today
      transformedProducts.forEach(product => {
        const hasOperation = opsData.some(op => op.product_id === product.id);
        if (!hasOperation) {
          // Get previous day's closing stock, fall back to current shelf_stock if not found
          const openingStock = previousClosingStockMap.get(product.id) ?? product.shelf_stock;
          const orderQty = todaySoldQtyMap.get(product.id) ?? 0;
          const estimatedClosing = computeEstimatedClosing(openingStock, 0, orderQty);
          const stolenStock = computeStolenStock(estimatedClosing, openingStock, 0);
          const unitPrice = product.unit_price || 0;
          const salesAmount = computeSalesAmount(unitPrice, orderQty);

          mergedOperations.push({
            id: undefined,
            product_id: product.id,
            product,
            opening_stock: openingStock,
            additional_stock: 0,
            actual_closing_stock: openingStock,
            estimated_closing_stock: estimatedClosing,
            stolen_stock: stolenStock,
            wastage_stock: 0,
            warehouse_stock: product.warehouse_stock,
            sales: salesAmount,
            order_count: orderQty,
            created_at: todayFormatted,
            updated_at: null,
          });
        }
      });


      setProducts(transformedProducts);
      setStockOperations(mergedOperations);
      stockOperationsRef.current = mergedOperations;
    } catch (error) {
      console.error('Error loading stock operations:', error);
      toast({
        title: 'Error',
        description: errorMessages.failedToLoadStockOperations,
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  }, [toast, errorMessages, computeEstimatedClosing, computeSalesAmount, computeStolenStock, getLocalDayRange]);

  const applyFilters = useCallback(() => {
    let filtered = [...stockOperations];

    if (filters.category && filters.category !== 'all') {
      filtered = filtered.filter(op => op.product?.category === filters.category);
    }

    if (filters.status && filters.status !== 'all') {
      filtered = filtered.filter(op => op.product?.status === filters.status);
    }

    if (filters.stockStatus && filters.stockStatus !== 'all') {
      if (filters.stockStatus === 'low') {
        filtered = filtered.filter(op => (op.opening_stock || 0) < 10);
      } else if (filters.stockStatus === 'out') {
        filtered = filtered.filter(op => (op.opening_stock || 0) === 0);
      }
    }

    return filtered;
  }, [stockOperations, filters]);

  useEffect(() => {
    loadStockOperations();
  }, [loadStockOperations]);

  useEffect(() => {
    const cleanup = setupRealtimeSubscription();

    // Track mouse and keyboard activity
    const handleActivity = () => trackUserActivity();
    document.addEventListener('mousemove', handleActivity);
    document.addEventListener('keypress', handleActivity);
    document.addEventListener('click', handleActivity);

    return () => {
      cleanup?.();
      document.removeEventListener('mousemove', handleActivity);
      document.removeEventListener('keypress', handleActivity);
      document.removeEventListener('click', handleActivity);
    };
  }, [setupRealtimeSubscription, trackUserActivity]);

  // Cleanup on component unmount
  useEffect(() => {
    return () => {
      // Clear audit timeout
      if (auditTimeoutRef.current) {
        clearTimeout(auditTimeoutRef.current);
      }

      // Clear all conflict warning timeouts
      conflictTimeoutsRef.current.forEach(timeoutId => {
        clearTimeout(timeoutId);
      });
      conflictTimeoutsRef.current = [];

      // Remove realtime channel
      if (realtimeChannelRef.current) {
        supabase.removeChannel(realtimeChannelRef.current);
      }
    };
  }, []);

  const filteredOperations = applyFilters();
  const hasValidationErrors = stockOperations.some(op => {
    const { exceedsEstimated, exceedsWithWastage } = getValidationIssues(op);
    return exceedsEstimated || exceedsWithWastage;
  });

  // Calculate grand total sales based on actual stock sold
  const grandTotalSales = filteredOperations.reduce((total, operation) => {
    const unitPrice = operation.product?.unit_price || operation.product?.price || 0;
    const sales = (operation.order_count || 0) * unitPrice;
    return total + sales;
  }, 0);

  const handleFilterChange = (filterType: keyof Filters, value: string) => {
    setFilters(prev => ({ ...prev, [filterType]: value }));
  };

  const updateOperation = useCallback((operationId: string | undefined, productId: string, updates: Partial<StockOperation>) => {
    setStockOperations(prev =>
      prev.map(op => {
        // For existing operations, match by ID
        if (operationId && op.id === operationId) {
          return { ...op, ...updates };
        }
        // For new operations (no ID yet), match by product_id
        if (!operationId && !op.id && op.product_id === productId) {
          return { ...op, ...updates };
        }
        return op;
      })
    );
  }, []);

  const handleOperationChange = (operationId: string | undefined, productId: string, field: keyof StockOperation, value: number) => {
    // Find operation by ID if it exists, otherwise by product_id for new operations
    const op = operationId
      ? stockOperations.find(op => op.id === operationId)
      : stockOperations.find(op => !op.id && op.product_id === productId);

    if (!op) return;

    // Validate input value
    const validatedValue = Math.max(0, Math.floor(value)); // Ensure non-negative integers
    const oldValue = op[field] as number;

    // Track user activity
    trackUserActivity();

    // Log audit trail for the change
    if (oldValue !== validatedValue) {
      logAuditTrail(
        'update',
        productId,
        op.product?.name || 'Unknown Product',
        field,
        oldValue,
        validatedValue
      );
    }

    const updatedOp: StockOperation = { ...op, [field]: validatedValue };

    // Recalculate estimated closing stock based on opening + additional - order_count
    if (['opening_stock', 'additional_stock', 'order_count'].includes(field)) {
      updatedOp.estimated_closing_stock = computeEstimatedClosing(
        updatedOp.opening_stock,
        updatedOp.additional_stock,
        updatedOp.order_count
      );
    }

    // Recalculate stolen stock based on estimated - actual - wastage
    if (['actual_closing_stock', 'wastage_stock', 'opening_stock', 'additional_stock', 'order_count'].includes(field)) {
      updatedOp.stolen_stock = computeStolenStock(
        updatedOp.estimated_closing_stock,
        updatedOp.actual_closing_stock,
        updatedOp.wastage_stock
      );
    }

    // Update sales amount when order count changes
    if (field === 'order_count') {
      const unitPrice = updatedOp.product?.unit_price || updatedOp.product?.price || 0;
      updatedOp.sales = computeSalesAmount(unitPrice, updatedOp.order_count);
    }

    // Auto-update actual closing stock if it matches the previous estimated value
    if (op.actual_closing_stock === op.estimated_closing_stock) {
      updatedOp.actual_closing_stock = updatedOp.estimated_closing_stock;
    }

    updateOperation(operationId, productId, updatedOp);
  };

  const handleSave = useCallback(async () => {
    if (!stockOperations.length) return;

    // Validate constraint: Actual + Wastage should not exceed Estimated
    const invalidOp = stockOperations.find(op => {
      const { estimated, exceedsEstimated, exceedsWithWastage } = getValidationIssues(op);
      return exceedsEstimated || exceedsWithWastage || (op.actual_closing_stock || 0) + (op.wastage_stock || 0) > estimated;
    });
    if (invalidOp) {
      toast({
        title: 'Validation Error',
        description: `Validation failed for ${invalidOp.product?.name || 'a product'}. Actual and Wastage must not exceed Estimated Closing.`,
        variant: 'destructive',
      });
      return;
    }

    setSaving(true);
    try {
      const todayFormatted = new Date().toISOString().split('T')[0];

      // Separate new operations from existing ones
      const newOperations: StockOperationRow[] = [];
      const existingOperations: StockOperationRow[] = [];

      stockOperations.forEach(({ product, ...op }) => {
        const unitPrice = product?.unit_price || product?.price || 0;
        const estimatedClosing = computeEstimatedClosing(op.opening_stock, op.additional_stock, op.order_count);
        const stolenStock = computeStolenStock(estimatedClosing, op.actual_closing_stock, op.wastage_stock);
        const salesAmount = computeSalesAmount(unitPrice, op.order_count);
        const cleanOp: StockOperationRow = {
          ...op,
          opening_stock: Number(op.opening_stock) || 0,
          additional_stock: Number(op.additional_stock) || 0,
          actual_closing_stock: Number(op.actual_closing_stock) || 0,
          estimated_closing_stock: Number(estimatedClosing) || 0,
          stolen_stock: Number(stolenStock) || 0,
          wastage_stock: Number(op.wastage_stock) || 0,
          sales: Number(salesAmount) || 0,
          order_count: Number(op.order_count) || 0,
          created_at: op.created_at || todayFormatted,
          updated_at: todayFormatted
        };

        if (op.id) {
          existingOperations.push(cleanOp);
        } else {
          newOperations.push(cleanOp);
        }
      });

      // Handle new operations (insert)
      if (newOperations.length > 0) {
        const { error: insertError } = await supabase
          .from('daily_stock_operations')
          .insert(newOperations);
        if (insertError) throw insertError;
      }

      // Handle existing operations (update)
      if (existingOperations.length > 0) {
        const { error: updateError } = await supabase
          .from('daily_stock_operations')
          .upsert(existingOperations, { onConflict: 'id' });
        if (updateError) throw updateError;
      }

      // Update product stocks with actual closing stock
      // Use Promise.all for concurrent execution but limit batch size to avoid overwhelming the server
      const batchSize = 10;
      const productUpdateBatches = [];

      for (let i = 0; i < stockOperations.length; i += batchSize) {
        const batch = stockOperations.slice(i, i + batchSize);
        const batchPromise = Promise.all(
          batch.map(async op => {
            const { error } = await supabase
              .from('products')
              .update({ shelf_stock: op.actual_closing_stock })
              .eq('id', op.product_id);

            if (error) {
              console.error(`Failed to update product ${op.product_id}:`, error);
              throw error;
            }
            return true;
          })
        );
        productUpdateBatches.push(batchPromise);
      }

      // Execute all batches with error handling
      try {
        await Promise.all(productUpdateBatches);
      } catch (batchError) {
        console.error('Some product updates failed:', batchError);
        // Continue with the rest of the save process, but log the error
      }

      // Log successful save to audit trail
      await auditLogger.current.log(
        'stock_operations_saved',
        'daily_stock_operations',
        {
          operations_count: stockOperations.length,
          new_operations: newOperations.length,
          updated_operations: existingOperations.length,
          user_id: user?.id,
          user_name: profile?.name || 'Unknown User',
          date: todayFormatted
        },
        'medium'
      );

      // Update local product state instead of full reload
      setProducts(prev =>
        prev.map(product => {
          const updatedOp = stockOperations.find(op => op.product_id === product.id);
          return updatedOp
            ? { ...product, shelf_stock: updatedOp.actual_closing_stock }
            : product;
        })
      );

      // Update stock operations with new IDs for newly created operations
      if (newOperations.length > 0) {
        // Only reload if we have new operations that need IDs
        loadStockOperations();
      }

      toast({
        title: 'Success',
        description: 'Stock operations saved successfully',
      });
    } catch (error) {
      console.error('Error saving operations:', error);
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : errorMessages.failedToSaveStockOperations,
        variant: 'destructive',
      });
    } finally {
      setSaving(false);
    }
  }, [stockOperations, toast, loadStockOperations, errorMessages, user?.id, profile?.name, computeEstimatedClosing, computeSalesAmount, computeStolenStock, getValidationIssues]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <LoadingSpinner text="Loading stock operations..." />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-gradient-to-r from-[#202072] to-[#e66166] text-white p-6 rounded-xl shadow-lg flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold mb-1 flex items-center gap-3">
            <Package className="h-8 w-8" />
            Daily Stock Accounting
          </h1>
          <p className="text-purple-100">Manage the stock operations for today</p>
        </div>
        <div className="flex items-center gap-4">
          {/* View Mode Toggle - Hidden on mobile */}
          <div className="hidden sm:flex items-center border border-white/20 rounded-lg p-1">
            <Button
              variant={viewMode === 'table' ? 'default' : 'ghost'}
              size="sm"
              onClick={() => setViewMode('table')}
              className="flex items-center gap-1 text-white hover:text-gray-900"
            >
              <List className="h-4 w-4" />
              Table
            </Button>
            <Button
              variant={viewMode === 'cards' ? 'default' : 'ghost'}
              size="sm"
              onClick={() => setViewMode('cards')}
              className="flex items-center gap-1 text-white hover:text-gray-900"
            >
              <Grid className="h-4 w-4" />
              Cards
            </Button>
          </div>

          {/* Active Users Indicator */}
          {activeUsers.length > 1 && (
            <div className="flex items-center gap-2 bg-white/20 px-3 py-2 rounded-lg">
              <Users className="h-4 w-4" />
              <span className="text-sm">{activeUsers.length - 1} other user{activeUsers.length > 2 ? 's' : ''} active</span>
            </div>
          )}

          {/* Real-time Status */}
          <div className="flex items-center gap-2 bg-green-500/20 px-3 py-2 rounded-lg">
            <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
            <span className="text-sm">Live</span>
          </div>
        </div>
      </div>

      {/* Conflict Warnings */}
      {conflictWarnings.map((warning, index) => (
        <Alert key={index} className="border-orange-200 bg-orange-50">
          <AlertTriangle className="h-4 w-4 text-orange-600" />
          <AlertDescription className="text-orange-800">
            {warning}. Your changes may conflict with recent updates.
          </AlertDescription>
        </Alert>
      ))}

      {/* Real-time Updates Panel */}
      {realtimeUpdates.length > 0 && (
        <Card className="border-blue-200 bg-blue-50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm flex items-center gap-2">
              <Clock className="h-4 w-4" />
              Recent Updates
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="space-y-2 max-h-32 overflow-y-auto">
              {realtimeUpdates.slice(0, 5).map((update) => (
                <div key={update.id} className="text-xs flex items-center justify-between p-2 bg-white rounded border">
                  <span>
                    <strong>{update.user_name}</strong> updated <strong>{update.product_name}</strong>
                    {' '}{update.field.replace('_', ' ')}: {update.old_value} → {update.new_value}
                  </span>
                  <Badge variant="outline" className="text-xs">
                    {new Date(update.timestamp).toLocaleTimeString()}
                  </Badge>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Filters Section */}
      <div className="grid grid-cols-3 gap-4">
        <Select
          value={filters.category}
          onValueChange={(value) => handleFilterChange('category', value)}
        >
          <SelectTrigger>
            <SelectValue placeholder="Category" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Categories</SelectItem>
            {PRODUCT_CATEGORIES.map((category) => (
              <SelectItem key={category} value={category}>
                {category}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select
          value={filters.status}
          onValueChange={(value) => handleFilterChange('status', value)}
        >
          <SelectTrigger>
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Status</SelectItem>
            {Array.from(new Set(products.map(p => p.status?.toString() || 'active'))).map((status) => (
              <SelectItem key={status} value={status}>
                {status}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select
          value={filters.stockStatus}
          onValueChange={(value) => handleFilterChange('stockStatus', value)}
        >
          <SelectTrigger>
            <SelectValue placeholder="Stock Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Stock Status</SelectItem>
            <SelectItem value="low">Low Stock</SelectItem>
            <SelectItem value="out">Out of Stock</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Stock Operations Display */}
      {viewMode === 'table' ? (
        <Card>
          <CardHeader className="pb-4">
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Stock Operations ({filteredOperations.length})</CardTitle>
                <CardDescription>Today's stock accounting overview</CardDescription>
              </div>
              <div className="text-right">
                <div className="text-2xl font-bold text-green-600">₹{grandTotalSales.toLocaleString()}</div>
                <div className="text-sm text-gray-500">Total Sales</div>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Product</TableHead>
                    <TableHead>Opening Stock</TableHead>
                    <TableHead>Additional Stock</TableHead>
                    <TableHead>Sold Qty</TableHead>
                    <TableHead>Estimated Closing Stock</TableHead>
                    <TableHead>Actual Closing Stock</TableHead>
                    <TableHead>Wastage</TableHead>
                    <TableHead>Stolen Stock</TableHead>
                    <TableHead>Sales</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredOperations.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={9} className="text-center py-8 text-gray-500">
                        No stock operations found for the selected filters.
                      </TableCell>
                    </TableRow>
                  ) : (
                    filteredOperations.map((operation, index) => {
                      // Calculate sales revenue from actual orders
                      const unitPrice = operation.product?.unit_price || operation.product?.price || 0;
                      const sales = computeSalesAmount(unitPrice, operation.order_count || 0);
                      // Calculate estimated closing stock (based on sold quantity)
                      const { estimated, exceedsEstimated, exceedsWithWastage } = getValidationIssues(operation);
                      const hasValidationError = exceedsEstimated || exceedsWithWastage;
                      const stolenStock = computeStolenStock(
                        estimated,
                        operation.actual_closing_stock || 0,
                        operation.wastage_stock || 0
                      );
                      return (
                        <TableRow
                          key={operation.id || `temp-${operation.product_id}-${index}`}
                          className={hasValidationError ? 'bg-red-50' : undefined}
                        >
                          <TableCell className="font-medium">
                            {operation.product?.name || 'Unknown Product'}
                            <br />
                            <small className="text-gray-500">Price: ₹{unitPrice}</small>
                          </TableCell>
                          <TableCell>{operation.opening_stock}</TableCell>
                          <TableCell>{operation.additional_stock}</TableCell>
                          <TableCell>{operation.order_count}</TableCell>
                          <TableCell className="text-right font-medium">{estimated}</TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              min="0"
                              value={operation.actual_closing_stock || ''}
                              onChange={(e) => handleOperationChange(operation.id, operation.product_id, 'actual_closing_stock', Number(e.target.value) || 0)}
                              className="text-right"
                            />
                            {exceedsEstimated && (
                              <div className="text-xs text-red-600 mt-1">
                                Actual exceeds estimated.
                              </div>
                            )}
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              min="0"
                              value={operation.wastage_stock || ''}
                              onChange={(e) => handleOperationChange(operation.id, operation.product_id, 'wastage_stock', Number(e.target.value) || 0)}
                              className="text-right"
                            />
                            {!exceedsEstimated && exceedsWithWastage && (
                              <div className="text-xs text-red-600 mt-1">
                                Actual + Wastage exceeds estimated.
                              </div>
                            )}
                          </TableCell>
                          <TableCell className="text-right font-medium">{stolenStock}</TableCell>
                          <TableCell>₹{sales.toLocaleString()}</TableCell>
                        </TableRow>
                      );
                    })
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      ) : (
        <Card>
          <CardHeader className="pb-4">
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Stock Operations ({filteredOperations.length})</CardTitle>
                <CardDescription>Today's stock accounting overview</CardDescription>
              </div>
              <div className="text-right">
                <div className="text-2xl font-bold text-green-600">₹{grandTotalSales.toLocaleString()}</div>
                <div className="text-sm text-gray-500">Total Sales</div>
              </div>
            </div>
          </CardHeader>
          <CardContent className="p-0">
            <StockOperationCardGrid
              operations={filteredOperations}
              onOperationChange={handleOperationChange}
              loading={loading}
            />
          </CardContent>
        </Card>
      )}

      {/* Save Button */}
      <div className="flex justify-end">
        {hasValidationErrors && (
          <div className="mr-4 text-sm text-red-600 self-center">
            Fix validation errors before saving.
          </div>
        )}
        <Button
          variant="default"
          onClick={handleSave}
          disabled={saving || hasValidationErrors}
        >
          {saving ? <Loader2 className="animate-spin" /> : <Save />}
          {saving ? 'Saving...' : 'Save Operations'}
        </Button>
      </div>
    </div>
  );
};

export default AdminStockAccounting;
