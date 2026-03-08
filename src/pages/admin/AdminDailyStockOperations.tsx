import React, { useState, useEffect, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useToast } from '@/hooks/use-toast';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Save, RefreshCw } from 'lucide-react';
import { LoadingSpinner } from '@/components/common/LoadingSpinner';

// --- Supabase Table Types ---
type ProductDB = {
  id: string;
  name: string;
  description?: string | null;
  price?: number | null;
  category: string;
  shelf_stock: number;
  warehouse_stock: number;
  created_at: string;
  created_by?: string | null;
  image_url?: string | null;
  is_archived?: boolean | null;
  status?: string | null;
  unit_price?: number | null;
  updated_at?: string | null;
  updated_by?: string | null;
};

type StockOperationDB = {
  id?: string;
  product_id: string;
  opening_stock?: number | null;
  additional_stock?: number | null;
  actual_closing_stock?: number | null;
  estimated_closing_stock?: number | null;
  stolen_stock?: number | null;
  wastage_stock?: number | null;
  sales?: number | null;
  order_count?: number | null;
  created_at?: string;
  updated_at?: string | null;
};

// --- UI Types ---
type Product = ProductDB;
type StockOperationUI = Omit<StockOperationDB, 'product_id'> & {
  product_id: string;
  opening_stock: number;
  additional_stock: number;
  actual_closing_stock: number;
  estimated_closing_stock: number;
  stolen_stock: number;
  wastage_stock: number;
  sales: number;
  order_count: number;
  created_at: string;
  updated_at: string | null;
  product: Product;
};

const AdminDailyStockOperations: React.FC = () => {
  const [operations, setOperations] = useState<StockOperationUI[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const { toast } = useToast();
  const today = new Date().toISOString().split('T')[0];
  const getLocalDayRange = () => {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const end = new Date(start);
    end.setDate(end.getDate() + 1);
    return { start, end };
  };

  // Load products and today's operations, then merge them
  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      // Fetch products
      const { data: products, error: productsError } = await supabase
        .from('products')
        .select('*')
        .order('name');

      if (productsError) throw productsError;
      if (!products) throw new Error('No products found');

      // Fetch today's stock operations
      const { start, end } = getLocalDayRange();
      const { data: opsRaw, error: opsError } = await supabase
        .from('daily_stock_operations')
        .select('*')
        .gte('created_at', start.toISOString())
        .lt('created_at', end.toISOString());
      const ops: StockOperationDB[] = opsRaw || [];

      if (opsError) throw opsError;

      // Merge into UI operations
      const merged: StockOperationUI[] = products.map((product): StockOperationUI => {
        const op = ops?.find((o) => o.product_id === product.id);

        const opening = op?.opening_stock ?? product.shelf_stock ?? 0;
        const additional = op?.additional_stock ?? 0;
        const sales = op?.sales ?? 0;
        const stolen = op?.stolen_stock ?? 0;
        const wastage = op?.wastage_stock ?? 0;
        const estimated = opening + additional - sales - stolen - wastage;
        const actual = op?.actual_closing_stock ?? product.shelf_stock ?? 0;

        return {
          id: op?.id ?? '',
          product_id: product.id,
          opening_stock: opening,
          additional_stock: additional,
          sales: sales,
          stolen_stock: stolen,
          wastage_stock: wastage,
          estimated_closing_stock: estimated,
          actual_closing_stock: actual,
          order_count: op?.order_count ?? 0,
          created_at: op?.created_at ?? today,
          updated_at: op?.updated_at ?? null,
          product,
        } as StockOperationUI;
      });

      setOperations(merged);
    } catch (error) {
      console.error('Error loading operations:', error);
      toast({
        title: 'Error',
        description: 'Failed to load products and stock data',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  }, [today, toast]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Handle input changes
  const handleOperationChange = (
    productId: string,
    field: keyof StockOperationUI,
    value: number
  ) => {
    if (value < 0) return;

    setOperations((prev) =>
      prev.map((op) => {
        if (op.product_id !== productId) return op;
        const updated: StockOperationUI = { ...op, [field]: value };

        // Recalculate estimated closing stock if relevant
        if (
          field === 'additional_stock' ||
          field === 'sales' ||
          field === 'stolen_stock' ||
          field === 'wastage_stock'
        ) {
          updated.estimated_closing_stock =
            updated.opening_stock +
            updated.additional_stock -
            updated.sales -
            updated.stolen_stock -
            updated.wastage_stock;

          // If actual closing matches previous estimated, auto-update it
          if (op.actual_closing_stock === op.estimated_closing_stock) {
            updated.actual_closing_stock = updated.estimated_closing_stock;
          }
        }
        return updated;
      })
    );
  };

  // Save all operations
  const handleSaveAll = async () => {
    setSaving(true);
    try {
      const now = new Date().toISOString();

      const toSave = operations.map((op) => ({
        id: op.id,
        product_id: op.product_id,
        opening_stock: op.opening_stock,
        additional_stock: op.additional_stock,
        actual_closing_stock: op.actual_closing_stock,
        estimated_closing_stock: op.estimated_closing_stock,
        stolen_stock: op.stolen_stock,
        wastage_stock: op.wastage_stock,
        sales: op.sales,
        order_count: op.order_count,
        created_at: op.created_at || today,
        updated_at: now,
      }));

      const { error } = await supabase
        .from('daily_stock_operations')
        .upsert(toSave, { onConflict: 'id', ignoreDuplicates: false });

      if (error) throw error;

      // Update product stocks
      const updates = operations.map((op) =>
        supabase
          .from('products')
          .update({
            shelf_stock: op.actual_closing_stock,
            updated_at: now,
          })
          .eq('id', op.product_id)
      );
      await Promise.all(updates);

      toast({
        title: 'Success',
        description: 'Stock operations saved successfully',
      });

      // Reload to get fresh data
      loadData();
    } catch (error) {
      console.error('Error saving operations:', error);
      toast({
        title: 'Error',
        description:
          error instanceof Error
            ? error.message
            : 'Failed to save stock operations',
        variant: 'destructive',
      });
    } finally {
      setSaving(false);
    }
  };

  // UI
  if (loading) {
    return <LoadingSpinner text="Loading stock operations..." />;
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-gradient-to-r from-[#202072] to-[#e66166] text-white p-6 rounded-xl shadow-lg flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold mb-1 flex items-center gap-3">
            <RefreshCw className="h-8 w-8" />
            Daily Stock Operations
          </h1>
          <p className="text-purple-100">
            Manage daily stock movements and update inventory
          </p>
        </div>
        <Button
          onClick={handleSaveAll}
          disabled={saving}
          variant="outline"
          className="flex items-center gap-2 rounded-xl border-white/50 text-white hover:border-white transition-all duration-200 backdrop-blur-md bg-white/20 hover:bg-white/30"
        >
          {saving ? (
            <>
              <RefreshCw className="h-4 w-4 animate-spin" />
              Saving...
            </>
          ) : (
            <>
              <Save className="h-4 w-4" />
              Save All
            </>
          )}
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>
            Stock Operations - {new Date(today).toLocaleDateString()}
          </CardTitle>
          <CardDescription>
            Update stock movements for the day. Closing stock and variance are calculated automatically.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="rounded-md border overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Product</TableHead>
                  <TableHead className="text-right">Opening Stock</TableHead>
                  <TableHead className="text-right">Additional Stock</TableHead>
                  <TableHead className="text-right">Sales</TableHead>
                  <TableHead className="text-right">Stolen</TableHead>
                  <TableHead className="text-right">Wastage</TableHead>
                  <TableHead className="text-right font-medium">Estimated Closing</TableHead>
                  <TableHead className="text-right font-medium">Actual Closing</TableHead>
                  <TableHead className="text-right font-medium">Variance</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {operations.map((op) => (
                  <TableRow key={op.product_id}>
                    <TableCell className="font-medium">
                      {op.product?.name || 'Unknown Product'}
                    </TableCell>
                    <TableCell>
                      <Input
                        type="number"
                        min="0"
                        value={op.opening_stock}
                        onChange={(e) =>
                          handleOperationChange(
                            op.product_id,
                            'opening_stock',
                            Number(e.target.value)
                          )
                        }
                        className="text-right"
                        disabled
                      />
                    </TableCell>
                    <TableCell>
                      <Input
                        type="number"
                        min="0"
                        value={op.additional_stock}
                        onChange={(e) =>
                          handleOperationChange(
                            op.product_id,
                            'additional_stock',
                            Number(e.target.value)
                          )
                        }
                        className="text-right"
                      />
                    </TableCell>
                    <TableCell>
                      <Input
                        type="number"
                        min="0"
                        value={op.sales}
                        onChange={(e) =>
                          handleOperationChange(
                            op.product_id,
                            'sales',
                            Number(e.target.value)
                          )
                        }
                        className="text-right"
                      />
                    </TableCell>
                    <TableCell>
                      <Input
                        type="number"
                        min="0"
                        value={op.stolen_stock}
                        onChange={(e) =>
                          handleOperationChange(
                            op.product_id,
                            'stolen_stock',
                            Number(e.target.value)
                          )
                        }
                        className="text-right"
                      />
                    </TableCell>
                    <TableCell>
                      <Input
                        type="number"
                        min="0"
                        value={op.wastage_stock}
                        onChange={(e) =>
                          handleOperationChange(
                            op.product_id,
                            'wastage_stock',
                            Number(e.target.value)
                          )
                        }
                        className="text-right"
                      />
                    </TableCell>
                    <TableCell className="text-right font-medium">
                      {op.estimated_closing_stock}
                    </TableCell>
                    <TableCell>
                      <Input
                        type="number"
                        min="0"
                        value={op.actual_closing_stock}
                        onChange={(e) =>
                          handleOperationChange(
                            op.product_id,
                            'actual_closing_stock',
                            Number(e.target.value)
                          )
                        }
                        className="text-right"
                      />
                    </TableCell>
                    <TableCell
                      className={`text-right font-medium ${op.estimated_closing_stock !== op.actual_closing_stock
                          ? 'text-red-500'
                          : ''
                        }`}
                    >
                      {op.estimated_closing_stock - op.actual_closing_stock}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default AdminDailyStockOperations;
