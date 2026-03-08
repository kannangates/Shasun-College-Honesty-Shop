import React, { useState, useEffect, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Package, Pencil, Plus, Grid, List } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/features/gamification/components/badge';
import { getGeneralStatusClass, getStockBadgeClass } from '@/utils/statusSystem';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { supabase } from '@/integrations/supabase/client';
import { LoadingSpinner } from '@/components/common/LoadingSpinner';
import { useToast } from '@/hooks/use-toast';
import { useAuth } from '@/hooks/useAuth';
import { useProductContext } from '@/contexts/useProductContext';

import { ProductFilters } from '@/components/product/ProductFilters';
import { AddProductModal } from './AddProductModal';
import { EditProductModal } from './EditProductModal';
import { RestockModal } from './RestockModal';
import { StudentCardGrid } from '@/components/product/StudentCardGrid';
import { useResponsiveView } from '@/hooks/useResponsiveView';

interface Product {
  id: string;
  name: string;
  category: string;
  unit_price: number;
  warehouse_stock: number;
  shelf_stock: number;
  status: string;
  created_at: string;
  updated_at: string;
  is_archived: boolean;
  image_url?: string;
  updated_by?: string;
}


export const AdminInventoryManagement = () => {
  useAuth();
  const { fetchProducts: refreshProductContext } = useProductContext();
  const [products, setProducts] = useState<Product[]>([]);
  const [filteredProducts, setFilteredProducts] = useState<Product[]>([]);
  const [loading, setLoading] = useState(true);
  const [lowStockProducts, setLowStockProducts] = useState<Product[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('all');
  const [selectedStatus, setSelectedStatus] = useState('active');
  const [stockFilter, setStockFilter] = useState('all');
  const [addModalOpen, setAddModalOpen] = useState(false);
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [selectedForEdit, setSelectedForEdit] = useState<Product | null>(null);
  const [restockModalOpen, setRestockModalOpen] = useState(false);
  const [selectedForRestock, setSelectedForRestock] = useState<Product | null>(null);
  const [addLoading, setAddLoading] = useState(false);
  const [viewMode, setViewMode] = useState<'table' | 'cards'>('table');
  const isMobile = useResponsiveView();

  const { toast } = useToast();

  // Auto-switch to cards view on mobile
  useEffect(() => {
    if (isMobile && viewMode === 'table') {
      setViewMode('cards');
    }
  }, [isMobile, viewMode]);

  const fetchProducts = useCallback(async () => {
    console.log('📦 Fetching products...');
    setLoading(true);
    try {
      const { data, error } = await supabase
        .from('products')
        .select('*')
        .order('created_at', { ascending: false });

      if (error) {
        console.error('Error fetching products:', error);
        toast({
          title: 'Error',
          description: 'Failed to fetch products',
          variant: 'destructive',
        });
        return;
      }

      console.log('📦 Products fetched:', data?.length || 0);
      setProducts(data || []);

    } catch (error) {
      console.error('Error in fetchProducts:', error);
      toast({
        title: 'Error',
        description: 'Failed to fetch products',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  }, [toast]);


  const fetchLowStockProducts = async () => {
    try {
      const { data, error } = await supabase
        .from('products')
        .select('*')
        .or('shelf_stock.lt.10,warehouse_stock.lt.10');

      if (error) {
        console.error('Error fetching low stock products:', error);
        return;
      }

      setLowStockProducts(data || []);
    } catch (error) {
      console.error('Error in fetchLowStockProducts:', error);
    }
  };

  useEffect(() => {
    fetchProducts();
    fetchLowStockProducts();
  }, [fetchProducts]);

  useEffect(() => {
    let filtered = products;

    // Apply status filter
    if (selectedStatus !== 'all') {
      filtered = filtered.filter(product => product.status === selectedStatus);
    }

    // Apply category filter
    if (selectedCategory !== 'all') {
      filtered = filtered.filter(product => product.category === selectedCategory);
    }

    // Apply search filter
    if (searchTerm) {
      filtered = filtered.filter(product =>
        product.name.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }

    // Apply stock filter
    if (stockFilter === 'in-stock') {
      filtered = filtered.filter(product => (product.shelf_stock || 0) > 0);
    } else if (stockFilter === 'low-stock') {
      filtered = filtered.filter(product => {
        const stock = product.shelf_stock || 0;
        return stock > 0 && stock <= 10;
      });
    } else if (stockFilter === 'out-of-stock') {
      filtered = filtered.filter(product => (product.shelf_stock || 0) === 0);
    }

    setFilteredProducts(filtered);
  }, [products, selectedCategory, selectedStatus, searchTerm, stockFilter]);




  const openEditModal = (product: Product) => {
    setSelectedForEdit(product);
    setEditModalOpen(true);
  };

  const closeEditModal = () => {
    setEditModalOpen(false);
    setSelectedForEdit(null);
  };

  const openRestockModal = (product: Product) => {
    setSelectedForRestock(product);
    setRestockModalOpen(true);
  };

  const closeRestockModal = () => {
    setRestockModalOpen(false);
    setSelectedForRestock(null);
  };

  const handleProductUpdate = async (id: string, updates: Partial<Product>) => {
    try {
      const mappedUpdates: Partial<Product> = {
        name: updates.name,
        category: updates.category,
        unit_price: (updates as Product & { price?: number }).price ?? updates.unit_price,
        image_url: updates.image_url,
        status: updates.status,
        updated_at: new Date().toISOString(),
      };

      const { error } = await supabase
        .from('products')
        .update(mappedUpdates)
        .eq('id', id);

      if (error) throw error;

      await fetchProducts();
      toast({ title: 'Updated', description: 'Product updated successfully' });
      closeEditModal();
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to update product';
      console.error('Error updating product:', err);
      toast({ title: 'Error', description: errorMessage, variant: 'destructive' });
      throw err;
    }
  };

  const handleStockUpdated = async () => {
    // Refresh products data after stock operations
    await fetchProducts();
  };

  const openAddModal = () => {
    setAddModalOpen(true);
  };

  const closeAddModal = () => {
    setAddModalOpen(false);
  };

  interface ProductData {
    name: string;
    price: number;
    image_url?: string;
    category: string;
    status: string;
    shelf_stock: number;
    warehouse_stock: number;
  }

  const handleAddProduct = async (productData: ProductData) => {
    setAddLoading(true);
    try {
      // Get current user from Supabase auth
      const { data: { user: currentUser } } = await supabase.auth.getUser();

      const { error } = await supabase
        .from('products')
        .insert([{
          name: productData.name,
          unit_price: productData.price,
          image_url: productData.image_url,
          category: productData.category,
          status: productData.status,
          shelf_stock: productData.shelf_stock,
          warehouse_stock: productData.warehouse_stock,
          is_archived: false,
          created_by: currentUser?.id || null,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        }]);

      if (error) throw error;

      await fetchProducts();
      await refreshProductContext(); // Refresh the ProductContext as well
      toast({ title: 'Success', description: 'Product added successfully' });
      closeAddModal();
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to add product';
      console.error('Error adding product:', err);
      toast({ title: 'Error', description: errorMessage, variant: 'destructive' });
      throw err;
    } finally {
      setAddLoading(false);
    }
  };



  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <LoadingSpinner />
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
            Inventory Management
          </h1>
          <p className="text-purple-100">
            Manage product stock levels and inventory operations
          </p>
        </div>
        <div className="flex items-center gap-2">
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
          <Button
            onClick={openAddModal}
            variant="outline"
            className="flex items-center gap-2 rounded-xl border-white/50 text-white hover:border-white transition-all duration-200 backdrop-blur-md bg-white/20 hover:bg-white/30"
          >
            <Plus className="h-4 w-4" />
            Add Product
          </Button>
        </div>
      </div>

      {/* Low Stock Alert removed as requested */}

      {/* Filters */}
      <ProductFilters
        searchTerm={searchTerm}
        onSearchChange={setSearchTerm}
        selectedCategory={selectedCategory}
        onCategoryChange={setSelectedCategory}
        selectedStatus={selectedStatus}
        onStatusChange={setSelectedStatus}
        stockFilter={stockFilter}
        onStockFilterChange={setStockFilter}
        outOfStockCount={lowStockProducts.length}
        lowStockCount={lowStockProducts.filter(p => {
          const stock = p.shelf_stock || 0;
          return stock > 0 && stock <= 10;
        }).length}
      />

      {/* Products Display */}
      {viewMode === 'table' ? (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Package className="h-5 w-5" />
              Products ({filteredProducts.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Product</TableHead>
                    <TableHead>Category</TableHead>
                    <TableHead>Price</TableHead>
                    <TableHead>Warehouse Stock</TableHead>
                    <TableHead>Shelf Stock</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredProducts.map((product) => (
                    <TableRow key={product.id}>
                      <TableCell className="font-medium">{product.name}</TableCell>
                      <TableCell>{product.category}</TableCell>
                      <TableCell>₹{product.unit_price}</TableCell>
                      <TableCell>
                        <Badge variant="outline" className={getStockBadgeClass(product.warehouse_stock || 0)}>
                          {product.warehouse_stock || 0}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline" className={getStockBadgeClass(product.shelf_stock || 0)}>
                          {product.shelf_stock || 0}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Badge className={getGeneralStatusClass(product.status)}>
                          {product.status}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => openRestockModal(product)}
                            className="flex items-center gap-2"
                          >
                            <Plus className="h-4 w-4" />
                            Restock
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => openEditModal(product)}
                            className="flex items-center gap-2"
                          >
                            <Pencil className="h-4 w-4" />
                            Edit
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>

            {filteredProducts.length === 0 && (
              <div className="text-center py-8 text-muted-foreground">
                No products found matching your filters.
              </div>
            )}
          </CardContent>
        </Card>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Package className="h-5 w-5" />
              Products ({filteredProducts.length})
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <StudentCardGrid
              products={filteredProducts}
              onEdit={openEditModal}
              onRestock={openRestockModal}
              loading={loading}
            />
          </CardContent>
        </Card>
      )}

      {/* Add Product Modal */}
      <AddProductModal
        isOpen={addModalOpen}
        onClose={closeAddModal}
        onAdd={handleAddProduct}
        loading={addLoading}
      />

      {/* Edit Product Modal */}
      <EditProductModal
        isOpen={editModalOpen}
        onClose={closeEditModal}
        product={selectedForEdit}
        onUpdate={handleProductUpdate}
      />

      {/* Restock Modal */}
      <RestockModal
        isOpen={restockModalOpen}
        onClose={closeRestockModal}
        product={selectedForRestock}
        onStockUpdated={handleStockUpdated}
      />
    </div>
  );
};
