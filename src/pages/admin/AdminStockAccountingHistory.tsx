import { useState, useEffect, useMemo, useCallback } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useToast } from '@/hooks/use-toast';
import { History, AlertTriangle, Loader2, Filter, Download } from 'lucide-react';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { supabase } from '@/integrations/supabase/client';
import { PRODUCT_CATEGORIES } from '@/constants/productCategories';

// Product interface (reused from existing pages)
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
}

// Stock operation from database
interface StockOperationDB {
  id: string;
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
  created_by?: string;
}

// Extended interface for UI display (includes product and operator info)
interface StockOperationHistoryRecord extends StockOperationDB {
  product: Product;
  operator_name?: string;
  variance: number; // Calculated: estimated_closing_stock - actual_closing_stock
}

// Filter state
interface HistoryFilters {
  startDate: string;
  endDate: string;
  category: string;
  operator: string;
  product: string;
}

// Summary statistics
interface SummaryStats {
  totalProducts: number;
  totalSalesValue: number;
  totalWastageUnits: number;
  totalStolenUnits: number;
}

const AdminStockAccountingHistory = () => {
  const { toast } = useToast();

  // State management
  const [records, setRecords] = useState<StockOperationHistoryRecord[]>([]);
  const [filteredRecords, setFilteredRecords] = useState<StockOperationHistoryRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [filters, setFilters] = useState<HistoryFilters>({
    startDate: '',
    endDate: '',
    category: 'all',
    operator: 'all',
    product: 'all',
  });
  const [summaryStats, setSummaryStats] = useState<SummaryStats>({
    totalProducts: 0,
    totalSalesValue: 0,
    totalWastageUnits: 0,
    totalStolenUnits: 0,
  });
  const [dateError, setDateError] = useState<string>('');
  const [showLargeRangeWarning, setShowLargeRangeWarning] = useState(false);

  // Date validation
  const validateDates = (startDate: string, endDate: string): boolean => {
    setDateError('');
    setShowLargeRangeWarning(false);

    if (!startDate || !endDate) {
      setDateError('Please select both start and end dates');
      return false;
    }

    // Create dates from the input strings (YYYY-MM-DD format)
    // Date inputs are in local timezone, so we parse them as local dates
    const today = new Date();
    const todayDateOnly = new Date(today.getFullYear(), today.getMonth(), today.getDate());

    const [startYear, startMonth, startDay] = startDate.split('-').map(Number);
    const start = new Date(startYear, startMonth - 1, startDay);

    const [endYear, endMonth, endDay] = endDate.split('-').map(Number);
    const end = new Date(endYear, endMonth - 1, endDay);

    // Check if start date is in the future
    if (start.getTime() > todayDateOnly.getTime()) {
      setDateError('Start date cannot be in the future');
      return false;
    }

    // Check if end date is before start date
    if (end.getTime() < start.getTime()) {
      setDateError('End date must be on or after start date');
      return false;
    }

    // Check for large date range (> 365 days)
    const daysDiff = Math.floor((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24));
    if (daysDiff > 365) {
      setShowLargeRangeWarning(true);
    }

    return true;
  };

  // Handle date change
  const handleDateChange = (field: 'startDate' | 'endDate', value: string) => {
    const newFilters = { ...filters, [field]: value };
    setFilters(newFilters);

    // Validate if both dates are set
    if (newFilters.startDate && newFilters.endDate) {
      validateDates(newFilters.startDate, newFilters.endDate);
    } else {
      setDateError('');
      setShowLargeRangeWarning(false);
    }
  };

  // Load stock operations from database
  const loadStockOperations = useCallback(async () => {
    if (!validateDates(filters.startDate, filters.endDate)) {
      return;
    }

    setLoading(true);
    try {
      // Query daily_stock_operations with date range filter
      const { data: operationsData, error: operationsError } = await supabase
        .from('daily_stock_operations')
        .select('*')
        .gte('created_at', filters.startDate)
        .lte('created_at', filters.endDate)
        .order('created_at', { ascending: false });

      if (operationsError) throw operationsError;

      // Get unique product IDs
      const productIds = [...new Set(operationsData?.map(op => op.product_id) || [])];

      // Fetch products
      const { data: productsData, error: productsError } = await supabase
        .from('products')
        .select('*')
        .in('id', productIds);

      if (productsError) throw productsError;

      // Get unique user IDs for operator names (from created_by)
      const userIds = [...new Set(
        operationsData?.map(op => (op as Record<string, unknown>).created_by as string).filter(Boolean) || []
      )];

      // Fetch user information for operators
      let usersMap = new Map<string, string>();
      if (userIds.length > 0) {
        const { data: usersData, error: usersError } = await supabase
          .from('users')
          .select('id, student_id')
          .in('id', userIds);

        if (usersError) {
          console.error('Error fetching users:', usersError);
          // Continue without operator names if users fetch fails
        } else {
          usersMap = new Map(
            usersData?.map(u => [u.id, u.student_id || u.id.substring(0, 8)]) || []
          );
        }
      }

      // Create products lookup map
      const productsMap = new Map(productsData?.map(p => [p.id, p]) || []);

      // Transform data to include product, operator name, and calculated variance
      const transformedRecords: StockOperationHistoryRecord[] = (operationsData || [])
        .map(op => {
          const product = productsMap.get(op.product_id);
          if (!product) return null;

          const variance = op.estimated_closing_stock - op.actual_closing_stock;

          // Get operator name from created_by
          const operatorId = (op as Record<string, unknown>).created_by as string | undefined;
          const operator_name = operatorId ? usersMap.get(operatorId) || 'Unknown' : 'System';

          return {
            id: op.id,
            product_id: op.product_id,
            opening_stock: op.opening_stock,
            additional_stock: op.additional_stock,
            actual_closing_stock: op.actual_closing_stock,
            estimated_closing_stock: op.estimated_closing_stock,
            stolen_stock: op.stolen_stock,
            wastage_stock: op.wastage_stock,
            warehouse_stock: op.warehouse_stock,
            sales: op.sales,
            order_count: op.order_count,
            created_at: op.created_at,
            updated_at: null,
            created_by: operatorId,
            product,
            operator_name,
            variance,
          } as StockOperationHistoryRecord;
        })
        .filter((record): record is StockOperationHistoryRecord => record !== null);

      setRecords(transformedRecords);
      setFilteredRecords(transformedRecords);

      toast({
        title: 'Success',
        description: `Loaded ${transformedRecords.length} stock operation records`,
      });
    } catch (error) {
      console.error('Error loading stock operations:', error);
      toast({
        title: 'Error',
        description: 'Failed to load stock operations data',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  }, [filters.startDate, filters.endDate, toast]);

  // Use centralized product categories
  const uniqueCategories = PRODUCT_CATEGORIES;

  // Get unique products from records
  const uniqueProducts = useMemo(() => {
    const productsMap = new Map<string, string>();
    records.forEach(r => {
      productsMap.set(r.product.id, r.product.name);
    });
    return Array.from(productsMap.entries())
      .map(([id, name]) => ({ id, name }))
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [records]);

  // Get unique operators from records
  const uniqueOperators = useMemo(() => {
    const operators = new Set(records.map(r => r.operator_name).filter(Boolean));
    return Array.from(operators).sort();
  }, [records]);

  // Apply filters to records and calculate summary statistics
  useEffect(() => {
    let filtered = [...records];

    // Apply category filter
    if (filters.category && filters.category !== 'all') {
      filtered = filtered.filter(r => r.product.category === filters.category);
    }

    // Apply operator filter
    if (filters.operator && filters.operator !== 'all') {
      filtered = filtered.filter(r => r.operator_name === filters.operator);
    }

    // Apply product filter
    if (filters.product && filters.product !== 'all') {
      filtered = filtered.filter(r => r.product.name === filters.product);
    }

    setFilteredRecords(filtered);

    // Calculate summary statistics from filtered records
    const uniqueProductIds = new Set(filtered.map(r => r.product_id));
    const totalProducts = uniqueProductIds.size;

    // sales column already contains the total sales value (order_count * unit_price)
    const totalSalesValue = filtered.reduce((sum, r) => sum + (r.sales || 0), 0);

    const totalWastageUnits = filtered.reduce((sum, r) => sum + r.wastage_stock, 0);
    const totalStolenUnits = filtered.reduce((sum, r) => sum + r.stolen_stock, 0);

    setSummaryStats({
      totalProducts,
      totalSalesValue,
      totalWastageUnits,
      totalStolenUnits,
    });
  }, [records, filters.category, filters.operator, filters.product]);

  // Handle filter change
  const handleFilterChange = (filterType: keyof HistoryFilters, value: string) => {
    setFilters(prev => ({ ...prev, [filterType]: value }));
  };

  // Handle CSV export
  const handleExport = () => {
    try {
      // Create CSV headers
      const headers = [
        'Date',
        'Product Name',
        'Opening Stock',
        'Additional Stock',
        'Sales',
        'Wastage',
        'Stolen Stock',
        'Estimated Closing Stock',
        'Actual Closing Stock',
        'Variance',
      ];

      // Create CSV rows
      const rows = filteredRecords.map(record => [
        new Date(record.created_at).toLocaleDateString(),
        record.product.name,
        record.opening_stock,
        record.additional_stock,
        record.sales,
        record.wastage_stock,
        record.stolen_stock,
        record.estimated_closing_stock,
        record.actual_closing_stock,
        record.variance,
      ]);

      // Combine headers and rows
      const csvContent = [
        headers.join(','),
        ...rows.map(row => row.map(cell => `"${cell}"`).join(',')),
      ].join('\n');

      // Create blob and download
      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);

      // Format filename with date range
      const filename = `stock-accounting-${filters.startDate}-to-${filters.endDate}.csv`;

      link.setAttribute('href', url);
      link.setAttribute('download', filename);
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      toast({
        title: 'Success',
        description: `Exported ${filteredRecords.length} records to ${filename}`,
      });
    } catch (error) {
      console.error('Error exporting data:', error);
      toast({
        title: 'Error',
        description: 'Failed to export data',
        variant: 'destructive',
      });
    }
  };

  // Handle load data
  const handleLoadData = () => {
    loadStockOperations();
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-gradient-to-r from-[#202072] to-[#e66166] text-white p-6 rounded-xl shadow-lg flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold mb-1 flex items-center gap-3">
            <History className="h-8 w-8" />
            Stock Accounting History
          </h1>
          <p className="text-purple-100">View and analyze historical stock operations across date ranges</p>
        </div>
      </div>

      {/* Date Range Filter Section */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-center gap-4">
            {/* Start Date */}
            <div className="flex items-center gap-2">
              <Label htmlFor="startDate" className="text-sm font-medium whitespace-nowrap">
                Start Date
              </Label>
              <Input
                id="startDate"
                type="date"
                value={filters.startDate}
                onChange={(e) => handleDateChange('startDate', e.target.value)}
                max={new Date().toISOString().split('T')[0]}
                aria-label="Select start date for stock operations history"
                aria-required="true"
                className="w-auto"
              />
            </div>

            {/* End Date */}
            <div className="flex items-center gap-2">
              <Label htmlFor="endDate" className="text-sm font-medium whitespace-nowrap">
                End Date
              </Label>
              <Input
                id="endDate"
                type="date"
                value={filters.endDate}
                onChange={(e) => handleDateChange('endDate', e.target.value)}
                max={new Date().toISOString().split('T')[0]}
                min={filters.startDate}
                aria-label="Select end date for stock operations history"
                aria-required="true"
                className="w-auto"
              />
            </div>

            {/* Load Data Button */}
            <Button
              onClick={handleLoadData}
              disabled={loading || !filters.startDate || !filters.endDate}
              className="bg-gradient-to-r from-[#202072] to-[#e66166]"
              aria-label="Load stock operations data for selected date range"
            >
              {loading ? 'Loading...' : 'Load Data'}
            </Button>
          </div>

          {/* Error Message */}
          {dateError && (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>{dateError}</AlertDescription>
            </Alert>
          )}

          {/* Large Range Warning */}
          {showLargeRangeWarning && !dateError && (
            <Alert>
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>
                Large date ranges may take longer to load
              </AlertDescription>
            </Alert>
          )}
        </CardContent>
      </Card>

      {/* Filters Section */}
      {records.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Filter className="h-5 w-5" />
              Filters
            </CardTitle>
            <CardDescription>Filter the displayed stock operations</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {/* Category Filter */}
              <div className="space-y-2">
                <Label htmlFor="categoryFilter">Category</Label>
                <Select
                  value={filters.category}
                  onValueChange={(value) => handleFilterChange('category', value)}
                >
                  <SelectTrigger id="categoryFilter" aria-label="Filter by product category">
                    <SelectValue placeholder="All Categories" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Categories</SelectItem>
                    {uniqueCategories.map((category) => (
                      <SelectItem key={category} value={category}>
                        {category}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Operator Filter */}
              <div className="space-y-2">
                <Label htmlFor="operatorFilter">Operator</Label>
                <Select
                  value={filters.operator}
                  onValueChange={(value) => handleFilterChange('operator', value)}
                >
                  <SelectTrigger id="operatorFilter" aria-label="Filter by operator">
                    <SelectValue placeholder="All Operators" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Operators</SelectItem>
                    {uniqueOperators.map((operator) => (
                      <SelectItem key={operator} value={operator}>
                        {operator}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Product Filter */}
              <div className="space-y-2">
                <Label htmlFor="productFilter">Product</Label>
                <Select
                  value={filters.product}
                  onValueChange={(value) => handleFilterChange('product', value)}
                >
                  <SelectTrigger id="productFilter" aria-label="Filter by product">
                    <SelectValue placeholder="All Products" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Products</SelectItem>
                    {uniqueProducts.map((product) => (
                      <SelectItem key={product.id} value={product.name}>
                        {product.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Summary Statistics */}
      {filteredRecords.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {/* Total Products Tracked */}
          <Card>
            <CardHeader className="pb-3">
              <CardDescription>Total Products Tracked</CardDescription>
              <CardTitle className="text-3xl">{summaryStats.totalProducts}</CardTitle>
            </CardHeader>
          </Card>

          {/* Total Sales Value */}
          <Card>
            <CardHeader className="pb-3">
              <CardDescription>Total Sales Value</CardDescription>
              <CardTitle className="text-3xl">
                ₹{summaryStats.totalSalesValue.toFixed(2)}
              </CardTitle>
            </CardHeader>
          </Card>

          {/* Total Wastage Units */}
          <Card>
            <CardHeader className="pb-3">
              <CardDescription>Total Wastage Units</CardDescription>
              <CardTitle className="text-3xl">{summaryStats.totalWastageUnits}</CardTitle>
            </CardHeader>
          </Card>

          {/* Total Stolen Units */}
          <Card>
            <CardHeader className="pb-3">
              <CardDescription>Total Stolen Units</CardDescription>
              <CardTitle className="text-3xl">{summaryStats.totalStolenUnits}</CardTitle>
            </CardHeader>
          </Card>
        </div>
      )}

      {/* Data Table Section */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Historical Stock Data</CardTitle>
              <CardDescription>
                {filteredRecords.length > 0
                  ? `Showing ${filteredRecords.length} stock operation record${filteredRecords.length !== 1 ? 's' : ''}`
                  : 'Select a date range and click "Load Data" to view stock operations'}
              </CardDescription>
            </div>
            {filteredRecords.length > 0 && (
              <Button
                onClick={handleExport}
                variant="outline"
                className="flex items-center gap-2"
                aria-label={`Export ${filteredRecords.length} stock operation records to CSV`}
              >
                <Download className="h-4 w-4" aria-hidden="true" />
                Export CSV
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
              <span className="ml-2">Loading stock operations...</span>
            </div>
          ) : filteredRecords.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-muted-foreground">
                {records.length === 0 && filters.startDate && filters.endDate
                  ? 'No stock operations found for the selected date range'
                  : 'Select a date range and click "Load Data" to view stock operations'}
              </p>
            </div>
          ) : (
            <div className="rounded-md border overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Date</TableHead>
                    <TableHead>Product Name</TableHead>
                    <TableHead className="text-right">Opening Stock</TableHead>
                    <TableHead className="text-right">Additional Stock</TableHead>
                    <TableHead className="text-right">Sales</TableHead>
                    <TableHead className="text-right">Wastage</TableHead>
                    <TableHead className="text-right">Stolen Stock</TableHead>
                    <TableHead className="text-right">Estimated Closing</TableHead>
                    <TableHead className="text-right">Actual Closing</TableHead>
                    <TableHead className="text-right">Variance</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredRecords.map((record) => (
                    <TableRow key={`${record.id}-${record.created_at}`}>
                      <TableCell className="font-medium">
                        {new Date(record.created_at).toLocaleDateString()}
                      </TableCell>
                      <TableCell>{record.product.name}</TableCell>
                      <TableCell className="text-right">{record.opening_stock}</TableCell>
                      <TableCell className="text-right">{record.additional_stock}</TableCell>
                      <TableCell className="text-right">{record.sales}</TableCell>
                      <TableCell className="text-right">{record.wastage_stock}</TableCell>
                      <TableCell className="text-right">{record.stolen_stock}</TableCell>
                      <TableCell className="text-right font-medium">
                        {record.estimated_closing_stock}
                      </TableCell>
                      <TableCell className="text-right font-medium">
                        {record.actual_closing_stock}
                      </TableCell>
                      <TableCell
                        className={`text-right font-medium ${record.variance !== 0 ? 'text-red-500' : ''
                          }`}
                      >
                        {record.variance}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default AdminStockAccountingHistory;
