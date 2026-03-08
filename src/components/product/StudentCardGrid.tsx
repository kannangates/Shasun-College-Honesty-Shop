import React from 'react';
import { StudentCard } from './StudentCard';

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

interface StudentCardGridProps {
  products: Product[];
  onEdit?: (product: Product) => void;
  onRestock?: (product: Product) => void;
  onAddToCart?: (product: Product) => void;
  loading?: boolean;
}

export const StudentCardGrid: React.FC<StudentCardGridProps> = ({
  products,
  onEdit,
  onRestock,
  onAddToCart,
  loading = false
}) => {
  if (loading) {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 p-4">
        {Array.from({ length: 8 }).map((_, index) => (
          <div
            key={index}
            className="bg-gray-200 animate-pulse rounded-lg h-64 w-full"
          />
        ))}
      </div>
    );
  }

  if (products.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-12 px-4">
        <div className="text-gray-400 mb-4">
          <svg
            className="w-16 h-16"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={1}
              d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2M4 13h2m13-8V4a1 1 0 00-1-1H7a1 1 0 00-1 1v1m8 0V4.5"
            />
          </svg>
        </div>
        <h3 className="text-lg font-medium text-gray-900 mb-2">No products found</h3>
        <p className="text-gray-500 text-center max-w-sm">
          No products match your current filters. Try adjusting your search criteria.
        </p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 p-4">
      {products.map((product) => (
        <StudentCard
          key={product.id}
          product={product}
          onEdit={onEdit}
          onRestock={onRestock}
          onAddToCart={onAddToCart}
        />
      ))}
    </div>
  );
};
