import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Package, TrendingDown, AlertTriangle } from 'lucide-react';

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
  opening_stock?: number;
  status?: string;
  unit_price?: number;
  updated_by?: string;
  updated_at?: string;
  [key: string]: string | number | boolean | undefined;
}

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

interface StockOperation extends StockOperationRow {
  product: Product;
}

interface StockOperationCardProps {
  operation: StockOperation;
  onOperationChange: (operationId: string | undefined, productId: string, field: keyof StockOperation, value: number) => void;
}

export const StockOperationCard: React.FC<StockOperationCardProps> = ({
  operation,
  onOperationChange
}) => {
  // Calculate sales revenue from actual orders
  const unitPrice = operation.product?.unit_price || operation.product?.price || 0;
  const sales = (operation.order_count || 0) * unitPrice;

  // Calculate estimated closing stock (based on order count in units)
  const estimatedClosingStock =
    (operation.opening_stock || 0) +
    (operation.additional_stock || 0) -
    (operation.order_count || 0);

  // Calculate stolen stock as estimated_closing_stock - actual_closing_stock - wastage_stock
  const stolenStock = Math.max(0,
    estimatedClosingStock - (operation.actual_closing_stock || 0) - (operation.wastage_stock || 0)
  );

  const isLowStock = (operation.opening_stock || 0) < 10;
  const hasWastage = (operation.wastage_stock || 0) > 0;
  const hasStolenStock = stolenStock > 0;
  const exceedsEstimated = (operation.actual_closing_stock || 0) > estimatedClosingStock;
  const exceedsWithWastage = (operation.actual_closing_stock || 0) + (operation.wastage_stock || 0) > estimatedClosingStock;
  const hasValidationError = exceedsEstimated || exceedsWithWastage;

  return (
    <Card className={`w-full bg-white shadow-sm hover:shadow-md transition-shadow duration-200 ${hasValidationError ? 'border-red-300 bg-red-50' : ''}`}>
      <CardHeader className="pb-2 px-4 pt-4">
        <div className="flex items-start gap-2">
          {/* Left Column - Image */}
          <div className="flex-shrink-0">
            {operation.product?.image_url ? (
              <img
                src={operation.product.image_url}
                alt={operation.product.name}
                className="w-12 h-12 rounded-md object-cover border border-gray-200"
              />
            ) : (
              <div className="w-12 h-12 rounded-md bg-gray-100 border border-gray-200 flex items-center justify-center">
                <Package className="h-5 w-5 text-gray-400" />
              </div>
            )}
          </div>

          {/* Right Column - Product Info */}
          <div className="flex-1 min-w-0 text-left">
            <div className="flex items-start justify-between gap-2">
              <div className="flex-1 min-w-0">
                <CardTitle className="text-base font-semibold text-gray-900 truncate leading-tight">
                  {operation.product?.name || 'Unknown Product'}
                </CardTitle>
                <p className="text-xs text-gray-500 mt-0.5">{operation.product?.category}</p>
              </div>
              {/* Status Indicators */}
              <div className="flex items-center gap-1 flex-shrink-0">
                {isLowStock && (
                  <div className="bg-red-100 border border-red-200 rounded-full p-0.5" title="Low Stock">
                    <AlertTriangle className="h-3 w-3 text-red-600" />
                  </div>
                )}
                {hasWastage && (
                  <div className="bg-orange-100 border border-orange-200 rounded-full p-0.5" title="Has Wastage">
                    <TrendingDown className="h-3 w-3 text-orange-600" />
                  </div>
                )}
                {hasStolenStock && (
                  <div className="bg-red-100 border border-red-200 rounded-full p-0.5" title="Stolen Stock Detected">
                    <AlertTriangle className="h-3 w-3 text-red-600" />
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </CardHeader>

      <CardContent className="pt-0 px-4 pb-4 space-y-3">
        {/* Stock Overview */}
        <div className="bg-blue-50 border border-blue-200 rounded-md p-2">
          <div className="grid grid-cols-2 gap-2">
            <div>
              <span className="text-xs font-medium text-blue-700">Opening</span>
              <div className="text-lg font-bold text-blue-600">{operation.opening_stock}</div>
            </div>
            <div>
              <span className="text-xs font-medium text-blue-700">Additional</span>
              <div className="text-lg font-bold text-blue-600">{operation.additional_stock}</div>
            </div>
          </div>
        </div>

        {/* Estimated vs Actual Closing Stock */}
        <div className="bg-green-50 border border-green-200 rounded-md p-2">
          <div className="grid grid-cols-2 gap-2">
            <div>
              <span className="text-xs font-medium text-green-700">Estimated</span>
              <div className="text-lg font-bold text-green-600">{estimatedClosingStock}</div>
            </div>
            <div>
              <span className="text-xs font-medium text-green-700">Actual</span>
              <Input
                type="number"
                min="0"
                value={operation.actual_closing_stock || ''}
                onChange={(e) => onOperationChange(operation.id, operation.product_id, 'actual_closing_stock', Number(e.target.value) || 0)}
                className="text-right font-bold text-green-600 border-green-300 focus:border-green-500 h-8 text-sm"
              />
              {exceedsEstimated && (
                <div className="text-xs text-red-600 mt-1">
                  Actual exceeds estimated.
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Loss Information */}
        <div className="bg-gray-50 rounded-md p-2">
          <div className="text-xs font-medium text-gray-700 mb-2">Loss Information</div>
          <div className="grid grid-cols-2 gap-2">
            <div className="bg-white rounded-sm p-1.5 border border-gray-200">
              <div className="text-xs text-gray-500 mb-1">Wastage</div>
              <Input
                type="number"
                min="0"
                value={operation.wastage_stock || ''}
                onChange={(e) => onOperationChange(operation.id, operation.product_id, 'wastage_stock', Number(e.target.value) || 0)}
                className="text-right text-xs h-7"
              />
              {!exceedsEstimated && exceedsWithWastage && (
                <div className="text-xs text-red-600 mt-1">
                  Actual + Wastage exceeds estimated.
                </div>
              )}
            </div>
            <div className="bg-white rounded-sm p-1.5 border border-gray-200">
              <div className="text-xs text-gray-500 mb-1">Stolen</div>
              <div className="text-right text-xs font-medium text-red-600 h-7 flex items-center justify-end">{stolenStock}</div>
            </div>
          </div>
        </div>

        {/* Sales Information */}
        <div className="bg-purple-50 border border-purple-200 rounded-md p-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-purple-700">Sales</span>
            <div className="text-lg font-bold text-purple-600">₹{sales.toLocaleString()}</div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};
