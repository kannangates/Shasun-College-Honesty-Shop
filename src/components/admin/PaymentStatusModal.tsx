import React, { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { CreditCard, AlertCircle } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { supabase } from '@/integrations/supabase/client';

interface PaymentStatusModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onStatusUpdated: () => void;
  orderData: {
    id: string;
    orderId: string;
    studentName: string;
    studentId: string;
    amount: number;
    currentStatus: string;
    transactionId?: string;
    paymentMethod?: string;
    paidAt?: string;
  } | null;
}

export const PaymentStatusModal = ({ open, onOpenChange, onStatusUpdated, orderData }: PaymentStatusModalProps) => {
  const { toast } = useToast();
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState({
    payment_status: '',
    payment_mode: '',
    transaction_id: '',
    paid_at: ''
  });
  const [errors, setErrors] = useState<Record<string, string>>({});

  // Initialize form data when orderData changes
  useEffect(() => {
    if (orderData) {
      setFormData({
        payment_status: orderData.currentStatus,
        payment_mode: orderData.paymentMethod || '',
        transaction_id: orderData.transactionId || '',
        paid_at: orderData.paidAt ? new Date(orderData.paidAt).toISOString().slice(0, 16) : new Date().toISOString().slice(0, 16)
      });
      setErrors({});
    }
  }, [orderData]);

  const validateForm = () => {
    const newErrors: Record<string, string> = {};

    if (!formData.payment_status) {
      newErrors.payment_status = 'Payment status is required';
    }

    if (formData.payment_status === 'paid') {
      if (!formData.payment_mode) {
        newErrors.payment_mode = 'Payment method is required for paid orders';
      }
      if (!formData.transaction_id.trim()) {
        newErrors.transaction_id = 'Transaction ID is required for paid orders';
      }
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!orderData || !validateForm()) return;

    setLoading(true);

    try {
      const updateData: Record<string, unknown> = {
        id: orderData.id,
        payment_status: formData.payment_status,
      };

      if (formData.payment_status === 'paid') {
        updateData.payment_mode = formData.payment_mode;
        updateData.transaction_id = formData.transaction_id;
        updateData.paid_at = new Date(formData.paid_at).toISOString();
      } else {
        // Clear payment details for non-paid statuses
        updateData.payment_mode = null;
        updateData.transaction_id = null;
        updateData.paid_at = null;
      }

      // Use edge function for admin updates (bypasses RLS)
      const response = await fetch(
        `${import.meta.env.VITE_SUPABASE_URL}/functions/v1/order-management`,
        {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${(await supabase.auth.getSession()).data.session?.access_token}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            operation: 'update_order',
            ...updateData,
          }),
        }
      );

      const responseData = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(responseData.error || 'Failed to update payment status');
      }

      if (!responseData?.order || responseData.order.payment_status !== formData.payment_status) {
        throw new Error('Payment status update did not persist. Please retry.');
      }

      toast({
        title: 'Success',
        description: `Payment status updated to ${formData.payment_status} successfully`,
      });

      onStatusUpdated();
      onOpenChange(false);
    } catch (error) {
      console.error('Error updating payment status:', error);
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to update payment status',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (field: string, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }));
    // Clear error when user starts typing
    if (errors[field]) {
      setErrors(prev => ({ ...prev, [field]: '' }));
    }
  };

  if (!orderData) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <CreditCard className="h-5 w-5" />
            Update Payment Status
          </DialogTitle>
          <DialogDescription>
            Update the payment status and transaction details for order {orderData.orderId}
          </DialogDescription>
        </DialogHeader>

        <div className="mb-4 p-3 bg-gray-50 rounded-md">
          <h4 className="font-medium text-sm mb-2">Order Details</h4>
          <div className="space-y-1 text-sm">
            <div><strong>Student:</strong> {orderData.studentName} ({orderData.studentId})</div>
            <div><strong>Amount:</strong> ₹{orderData.amount}</div>
            <div><strong>Current Status:</strong> <span className="capitalize">{orderData.currentStatus}</span></div>
          </div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <Label htmlFor="payment_status">Payment Status *</Label>
            <Select
              value={formData.payment_status}
              onValueChange={(value) => handleChange('payment_status', value)}
            >
              <SelectTrigger className={errors.payment_status ? 'border-red-500' : ''}>
                <SelectValue placeholder="Select payment status" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="unpaid">Unpaid</SelectItem>
                <SelectItem value="paid">Paid</SelectItem>
                <SelectItem value="pending">Pending</SelectItem>
                <SelectItem value="refunded">Refunded</SelectItem>
              </SelectContent>
            </Select>
            {errors.payment_status && (
              <div className="flex items-center gap-1 text-sm text-red-600 mt-1">
                <AlertCircle className="h-4 w-4" />
                {errors.payment_status}
              </div>
            )}
          </div>

          {formData.payment_status === 'paid' && (
            <>
              <div>
                <Label htmlFor="payment_mode">Payment Method *</Label>
                <Select
                  value={formData.payment_mode}
                  onValueChange={(value) => handleChange('payment_mode', value)}
                >
                  <SelectTrigger className={errors.payment_mode ? 'border-red-500' : ''}>
                    <SelectValue placeholder="Select payment method" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="qr_manual">QR Manual</SelectItem>
                    <SelectItem value="razorpay">Razorpay</SelectItem>
                    <SelectItem value="pay_later">Pay Later</SelectItem>
                    <SelectItem value="cash">Cash</SelectItem>
                    <SelectItem value="upi">UPI</SelectItem>
                    <SelectItem value="card">Card</SelectItem>
                  </SelectContent>
                </Select>
                {errors.payment_mode && (
                  <div className="flex items-center gap-1 text-sm text-red-600 mt-1">
                    <AlertCircle className="h-4 w-4" />
                    {errors.payment_mode}
                  </div>
                )}
              </div>

              <div>
                <Label htmlFor="transaction_id">Transaction ID / Reference *</Label>
                <Input
                  id="transaction_id"
                  value={formData.transaction_id}
                  onChange={(e) => handleChange('transaction_id', e.target.value)}
                  placeholder="Enter transaction ID or reference number"
                  className={errors.transaction_id ? 'border-red-500' : ''}
                />
                {errors.transaction_id && (
                  <div className="flex items-center gap-1 text-sm text-red-600 mt-1">
                    <AlertCircle className="h-4 w-4" />
                    {errors.transaction_id}
                  </div>
                )}
              </div>

              <div>
                <Label htmlFor="paid_at">Payment Date & Time</Label>
                <Input
                  id="paid_at"
                  type="datetime-local"
                  value={formData.paid_at}
                  onChange={(e) => handleChange('paid_at', e.target.value)}
                />
              </div>
            </>
          )}

          <div className="flex gap-2 pt-4">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={loading} className="flex-1">
              {loading ? 'Updating...' : 'Update Status'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
};
