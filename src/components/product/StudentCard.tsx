import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/features/gamification/components/badge';
import { Button } from '@/components/ui/button';
import { Package, Pencil, Plus, ShoppingCart } from 'lucide-react';
import { getGeneralStatusClass, getStockBadgeClass, getStockBadgeLabel } from '@/utils/statusSystem';

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

interface StudentCardProps {
  product: Product;
  onEdit?: (product: Product) => void;
  onRestock?: (product: Product) => void;
  onAddToCart?: (product: Product) => void;
}

export const StudentCard: React.FC<StudentCardProps> = ({
  product,
  onEdit,
  onRestock,
  onAddToCart
}) => {


  const totalStock = (product.warehouse_stock || 0) + (product.shelf_stock || 0);
  const isLowStock = totalStock < 10;
  const isOutOfStock = totalStock === 0;

  return (
    <Card className="w-full max-w-sm mx-auto bg-white shadow-md hover:shadow-lg transition-shadow duration-200">
      <CardHeader className="pb-3">
        <div className="flex items-start gap-3">
          {/* Left Column - Image */}
          <div className="flex-shrink-0">
            {product.image_url ? (
              <img
                src={product.image_url}
                alt={product.name}
                className="w-16 h-16 rounded-lg object-cover border border-gray-200"
              />
            ) : (
              <div className="w-16 h-16 rounded-lg bg-gray-100 border border-gray-200 flex items-center justify-center">
                <Package className="h-6 w-6 text-gray-400" />
              </div>
            )}
          </div>

          {/* Right Column - Product Info */}
          <div className="flex-1 min-w-0 text-left">
            <div className="flex items-start justify-between gap-2">
              <div className="flex-1 min-w-0">
                <CardTitle className="text-lg font-semibold text-gray-900 truncate">
                  {product.name}
                </CardTitle>
                <p className="text-sm text-gray-500 mt-1">{product.category}</p>
              </div>
              {/* Status Badge with Low Stock Alert */}
              <div className="flex items-center gap-2 flex-shrink-0">
                {isLowStock && (
                  <div className="bg-red-100 border border-red-200 rounded-full p-1">
                    <Package className="h-4 w-4 text-red-600" />
                  </div>
                )}
                <Badge
                  variant="outline"
                  className={`text-xs ${getGeneralStatusClass(product.status)}`}
                >
                  {product.status}
                </Badge>
              </div>
            </div>
          </div>
        </div>
      </CardHeader>

      <CardContent className="pt-0 space-y-4">
        {/* Price */}
        <div className="bg-green-50 border border-green-200 rounded-lg p-3">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium text-green-700">Price</span>
            <span className="text-xl font-bold text-green-600">₹{product.unit_price}</span>
          </div>
        </div>

        {/* Stock Information */}
        <div className="bg-gray-50 rounded-lg p-3 space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium text-gray-700">Total Stock</span>
            <Badge
              variant="outline"
              className={`text-sm font-semibold ${getStockBadgeClass(totalStock)}`}
            >
              {totalStock}
            </Badge>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="bg-white rounded-md p-2 border border-gray-200">
              <div className="text-xs text-gray-500 mb-1">Warehouse</div>
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-900">{product.warehouse_stock || 0}</span>
                <Badge variant="outline" className={`text-xs ${getStockBadgeClass(product.warehouse_stock || 0)}`}>
                  {getStockBadgeLabel(product.warehouse_stock || 0)}
                </Badge>
              </div>
            </div>
            <div className="bg-white rounded-md p-2 border border-gray-200">
              <div className="text-xs text-gray-500 mb-1">Shelf</div>
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-900">{product.shelf_stock || 0}</span>
                <Badge variant="outline" className={`text-xs ${getStockBadgeClass(product.shelf_stock || 0)}`}>
                  {getStockBadgeLabel(product.shelf_stock || 0)}
                </Badge>
              </div>
            </div>
          </div>
        </div>



        {/* Action Buttons */}
        <div className="flex flex-col gap-3 pt-2">
          {onAddToCart && !isOutOfStock && (
            <Button
              onClick={() => onAddToCart(product)}
              className="w-full flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-700 active:bg-blue-800 h-10 text-sm font-medium"
              size="sm"
            >
              <ShoppingCart className="h-4 w-4" />
              Add to Cart
            </Button>
          )}

          <div className="flex gap-3">
            {onEdit && (
              <Button
                onClick={() => onEdit(product)}
                className="flex-1 flex items-center justify-center gap-2 bg-gray-600 hover:bg-gray-700 active:bg-gray-800 text-white border-0 h-10 text-sm font-medium"
                size="sm"
              >
                <Pencil className="h-4 w-4" />
                Edit
              </Button>
            )}

            {onRestock && (
              <Button
                onClick={() => onRestock(product)}
                className="flex-1 flex items-center justify-center gap-2 bg-orange-500 hover:bg-orange-600 active:bg-orange-700 text-white border-0 h-10 text-sm font-medium"
                size="sm"
              >
                <Plus className="h-4 w-4" />
                Restock
              </Button>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
};
