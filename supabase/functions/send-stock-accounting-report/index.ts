import { createClient, SupabaseClient } from 'https://esm.sh/@supabase/supabase-js@2';
import { z } from 'https://deno.land/x/zod@v3.22.4/mod.ts';
import { corsHeaders } from '../_shared/cors.ts';

const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;
const REPORT_KEY = 'stock_accounting_history';

type Frequency = 'daily' | 'weekly' | 'monthly';

interface StockOperationRow {
  id: string;
  product_id: string;
  opening_stock: number;
  additional_stock: number;
  actual_closing_stock: number;
  wastage_stock: number;
  order_count: number;
  sales: number;
  created_at: string;
  created_by?: string | null;
}

interface ProductRow {
  id: string;
  name: string;
  category: string;
  unit_price?: number | null;
  price?: number | null;
}

interface ReportRow {
  date: string;
  productName: string;
  category: string;
  operator: string;
  openingStock: number;
  additionalStock: number;
  soldQty: number;
  estimatedClosingStock: number;
  actualClosingStock: number;
  wastageStock: number;
  stolenStock: number;
  salesValue: number;
  productId: string;
  unitPrice: number;
}

const manualSendSchema = z.object({
  operation: z.literal('manual_send'),
  startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'startDate must be YYYY-MM-DD'),
  endDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'endDate must be YYYY-MM-DD'),
  recipients: z.array(z.string().email()).min(1, 'At least one recipient is required'),
  filters: z.object({
    category: z.string().optional(),
    operator: z.string().optional(),
    product: z.string().optional(),
  }).optional(),
});

const scheduledDispatchSchema = z.object({
  operation: z.literal('scheduled_dispatch'),
});

const requestSchema = z.discriminatedUnion('operation', [manualSendSchema, scheduledDispatchSchema]);

const formatDate = (date: Date): string => {
  const year = date.getUTCFullYear();
  const month = `${date.getUTCMonth() + 1}`.padStart(2, '0');
  const day = `${date.getUTCDate()}`.padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const toBase64Utf8 = (content: string): string => {
  const bytes = new TextEncoder().encode(content);
  let binary = '';
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize);
    binary += String.fromCharCode(...chunk);
  }
  return btoa(binary);
};

const toIstDate = (date = new Date()): Date => new Date(date.getTime() + IST_OFFSET_MS);

const getPreviousDayRange = (istNow: Date) => {
  const y = new Date(Date.UTC(istNow.getUTCFullYear(), istNow.getUTCMonth(), istNow.getUTCDate() - 1));
  return { startDate: formatDate(y), endDate: formatDate(y) };
};

const getPreviousWeekRange = (istNow: Date) => {
  const dayOfWeek = istNow.getUTCDay(); // 0=Sun ... 6=Sat
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  const currentMonday = new Date(Date.UTC(istNow.getUTCFullYear(), istNow.getUTCMonth(), istNow.getUTCDate() - daysSinceMonday));
  const prevMonday = new Date(currentMonday);
  prevMonday.setUTCDate(prevMonday.getUTCDate() - 7);
  const prevSunday = new Date(currentMonday);
  prevSunday.setUTCDate(prevSunday.getUTCDate() - 1);
  return { startDate: formatDate(prevMonday), endDate: formatDate(prevSunday) };
};

const getPreviousMonthRange = (istNow: Date) => {
  const start = new Date(Date.UTC(istNow.getUTCFullYear(), istNow.getUTCMonth() - 1, 1));
  const end = new Date(Date.UTC(istNow.getUTCFullYear(), istNow.getUTCMonth(), 0));
  return { startDate: formatDate(start), endDate: formatDate(end) };
};

const parseTimeToMinutes = (timeText: string): number => {
  const [hoursText, minutesText] = timeText.split(':');
  const hours = Number(hoursText);
  const minutes = Number(minutesText);
  return hours * 60 + minutes;
};

const getIstMinutes = (istNow: Date): number => {
  return (istNow.getUTCHours() * 60) + istNow.getUTCMinutes();
};

const getCsvContent = (rows: ReportRow[]): string => {
  const headers = [
    'Date',
    'Product Name',
    'Category',
    'Operator',
    'Opening Stock',
    'Additional Stock',
    'Sold Qty',
    'Estimated Closing Stock',
    'Actual Closing Stock',
    'Wastage Stock',
    'Stolen Stock',
    'Sales Value',
  ];

  const csvRows = rows.map((row) => [
    row.date,
    row.productName,
    row.category,
    row.operator,
    row.openingStock,
    row.additionalStock,
    row.soldQty,
    row.estimatedClosingStock,
    row.actualClosingStock,
    row.wastageStock,
    row.stolenStock,
    row.salesValue.toFixed(2),
  ]);

  return [
    headers.join(','),
    ...csvRows.map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(',')),
  ].join('\n');
};

const getSummary = (rows: ReportRow[]) => {
  const uniqueProductIds = new Set(rows.map((r) => r.productId));
  const totalSalesValue = rows.reduce((sum, r) => sum + r.salesValue, 0);
  const totalWastageUnits = rows.reduce((sum, r) => sum + r.wastageStock, 0);
  const totalStolenUnits = rows.reduce((sum, r) => sum + r.stolenStock, 0);
  const totalWastageValue = rows.reduce((sum, r) => sum + (r.wastageStock * r.unitPrice), 0);
  const totalStolenValue = rows.reduce((sum, r) => sum + (r.stolenStock * r.unitPrice), 0);

  return {
    totalProducts: uniqueProductIds.size,
    totalRows: rows.length,
    totalSalesValue,
    totalWastageUnits,
    totalStolenUnits,
    totalWastageValue,
    totalStolenValue,
  };
};

const getRoleAccess = async (supabase: SupabaseClient, userId: string): Promise<boolean> => {
  const { data: userRoles, error } = await supabase
    .from('user_roles')
    .select('role')
    .eq('user_id', userId);

  if (error) {
    console.error('Failed to load user roles:', error);
    return false;
  }

  const roles = (userRoles || []).map((r) => r.role);
  return roles.includes('admin') || roles.includes('developer');
};

const getOperationRows = async (
  supabase: SupabaseClient,
  startDate: string,
  endDate: string,
): Promise<ReportRow[]> => {
  const { data: operationsData, error: operationsError } = await supabase
    .from('daily_stock_operations')
    .select('id, product_id, opening_stock, additional_stock, actual_closing_stock, wastage_stock, order_count, sales, created_at, created_by')
    .gte('created_at', startDate)
    .lte('created_at', endDate)
    .order('created_at', { ascending: true });

  if (operationsError) throw operationsError;

  const operationRows = (operationsData || []) as StockOperationRow[];
  if (!operationRows.length) {
    return [];
  }

  const productIds = [...new Set(operationRows.map((op) => op.product_id).filter(Boolean))];
  if (!productIds.length) {
    return [];
  }
  const createdByIds = [...new Set(operationRows.map((op) => op.created_by).filter(Boolean))] as string[];

  const [{ data: productsData, error: productsError }, { data: usersData, error: usersError }] = await Promise.all([
    supabase.from('products').select('id, name, category, unit_price, price').in('id', productIds),
    createdByIds.length > 0
      ? supabase.from('users').select('id, student_id').in('id', createdByIds)
      : Promise.resolve({ data: [], error: null }),
  ]);

  if (productsError) throw productsError;
  if (usersError) throw usersError;

  const productsMap = new Map((productsData || []).map((p: ProductRow) => [p.id, p]));
  const usersMap = new Map((usersData || []).map((u: { id: string; student_id?: string | null }) => [u.id, u.student_id || 'Unknown']));

  return operationRows
    .map((op) => {
      const product = productsMap.get(op.product_id);
      if (!product) return null;

      const soldQty = op.order_count || 0;
      const unitPrice = Number(product.unit_price || product.price || 0);
      const estimatedClosingStock = (op.opening_stock || 0) + (op.additional_stock || 0) - soldQty;
      const stolenStock = Math.max(0, estimatedClosingStock - (op.actual_closing_stock || 0) - (op.wastage_stock || 0));
      const salesValue = (op.sales && op.sales > 0) ? op.sales : soldQty * unitPrice;
      const operator = op.created_by ? (usersMap.get(op.created_by) || 'Unknown') : 'System';
      const date = new Date(op.created_at).toISOString().split('T')[0];

      return {
        date,
        productName: product.name,
        category: product.category,
        operator,
        openingStock: op.opening_stock || 0,
        additionalStock: op.additional_stock || 0,
        soldQty,
        estimatedClosingStock,
        actualClosingStock: op.actual_closing_stock || 0,
        wastageStock: op.wastage_stock || 0,
        stolenStock,
        salesValue,
        productId: op.product_id,
        unitPrice,
      } as ReportRow;
    })
    .filter((row): row is ReportRow => row !== null);
};

const applyFilters = (
  rows: ReportRow[],
  filters?: { category?: string; operator?: string; product?: string },
): ReportRow[] => {
  let filtered = [...rows];
  if (filters?.category && filters.category !== 'all') {
    filtered = filtered.filter((r) => r.category === filters.category);
  }
  if (filters?.operator && filters.operator !== 'all') {
    filtered = filtered.filter((r) => r.operator === filters.operator);
  }
  if (filters?.product && filters.product !== 'all') {
    filtered = filtered.filter((r) => r.productName === filters.product);
  }
  return filtered;
};

const sendReportEmail = async (
  supabaseUrl: string,
  serviceRoleKey: string,
  recipients: string[],
  subject: string,
  htmlBody: string,
  plainTextBody: string,
  csvFilename: string,
  csvContent: string,
) => {
  const response = await fetch(`${supabaseUrl}/functions/v1/send-email`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${serviceRoleKey}`,
      apikey: serviceRoleKey,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      to: recipients[0],
      cc: recipients.slice(1),
      subject,
      htmlBody,
      plainTextBody,
      attachments: [
        {
          filename: csvFilename,
          contentType: 'text/csv',
          base64Content: toBase64Utf8(csvContent),
        },
      ],
    }),
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload?.success === false) {
    throw new Error(payload?.error || 'Failed to send stock accounting report email');
  }
};

const getSubject = (frequency: Frequency | 'manual', startDate: string, endDate: string): string => {
  const freqLabel = frequency === 'manual'
    ? 'Manual'
    : frequency.charAt(0).toUpperCase() + frequency.slice(1);
  return `Stock Accounting Report (${freqLabel}) - ${startDate} to ${endDate}`;
};

const getBodyContent = (
  summary: ReturnType<typeof getSummary>,
  startDate: string,
  endDate: string,
  filtersText: string,
) => {
  const plainTextBody = [
    'Stock Accounting Report',
    `Period: ${startDate} to ${endDate}`,
    `Filters: ${filtersText}`,
    `Products Tracked: ${summary.totalProducts}`,
    `Rows: ${summary.totalRows}`,
    `Total Sales Value: ₹${summary.totalSalesValue.toFixed(2)}`,
    `Total Wastage: ${summary.totalWastageUnits} units (₹${summary.totalWastageValue.toFixed(2)})`,
    `Total Stolen: ${summary.totalStolenUnits} units (₹${summary.totalStolenValue.toFixed(2)})`,
    '',
    'Please find the detailed CSV attached.',
  ].join('\n');

  const htmlBody = `
    <div style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
      <h2 style="color:#202072; margin-bottom:8px;">Stock Accounting Report</h2>
      <p style="margin:0 0 12px 0;"><strong>Period:</strong> ${startDate} to ${endDate}</p>
      <p style="margin:0 0 12px 0;"><strong>Filters:</strong> ${filtersText}</p>
      <table style="border-collapse: collapse; width: 100%; margin-top: 8px;">
        <tr>
          <td style="padding:8px;border:1px solid #e5e7eb;"><strong>Products Tracked</strong></td>
          <td style="padding:8px;border:1px solid #e5e7eb;">${summary.totalProducts}</td>
        </tr>
        <tr>
          <td style="padding:8px;border:1px solid #e5e7eb;"><strong>Rows</strong></td>
          <td style="padding:8px;border:1px solid #e5e7eb;">${summary.totalRows}</td>
        </tr>
        <tr>
          <td style="padding:8px;border:1px solid #e5e7eb;"><strong>Total Sales Value</strong></td>
          <td style="padding:8px;border:1px solid #e5e7eb;">₹${summary.totalSalesValue.toFixed(2)}</td>
        </tr>
        <tr>
          <td style="padding:8px;border:1px solid #e5e7eb;"><strong>Total Wastage</strong></td>
          <td style="padding:8px;border:1px solid #e5e7eb;">${summary.totalWastageUnits} units (₹${summary.totalWastageValue.toFixed(2)})</td>
        </tr>
        <tr>
          <td style="padding:8px;border:1px solid #e5e7eb;"><strong>Total Stolen</strong></td>
          <td style="padding:8px;border:1px solid #e5e7eb;">${summary.totalStolenUnits} units (₹${summary.totalStolenValue.toFixed(2)})</td>
        </tr>
      </table>
      <p style="margin-top:16px;color:#6b7280;">Detailed rows are attached as CSV.</p>
    </div>
  `;

  return { plainTextBody, htmlBody };
};

const handleManualSend = async (
  supabase: SupabaseClient,
  supabaseUrl: string,
  serviceRoleKey: string,
  payload: z.infer<typeof manualSendSchema>,
) => {
  const allRows = await getOperationRows(supabase, payload.startDate, payload.endDate);
  const filteredRows = applyFilters(allRows, payload.filters);
  const summary = getSummary(filteredRows);
  const filtersText = `category=${payload.filters?.category || 'all'}, operator=${payload.filters?.operator || 'all'}, product=${payload.filters?.product || 'all'}`;
  const subject = getSubject('manual', payload.startDate, payload.endDate);
  const csvContent = getCsvContent(filteredRows);
  const csvFilename = `stock-accounting-${payload.startDate}-to-${payload.endDate}.csv`;
  const { plainTextBody, htmlBody } = getBodyContent(summary, payload.startDate, payload.endDate, filtersText);

  await sendReportEmail(
    supabaseUrl,
    serviceRoleKey,
    payload.recipients,
    subject,
    htmlBody,
    plainTextBody,
    csvFilename,
    csvContent,
  );

  return {
    success: true,
    recipients: payload.recipients.length,
    rows: filteredRows.length,
    period: { startDate: payload.startDate, endDate: payload.endDate },
  };
};

const runSchedule = async (
  supabase: SupabaseClient,
  supabaseUrl: string,
  serviceRoleKey: string,
  schedule: {
    id: string;
    frequency: Frequency;
    recipients: string[] | null;
    send_time: string;
    enabled: boolean;
    last_sent_for_period: string | null;
  },
  period: { startDate: string; endDate: string },
) => {
  const recipients = (schedule.recipients || []).filter(Boolean);
  if (!recipients.length) {
    await supabase.from('report_email_runs').insert({
      schedule_id: schedule.id,
      report_key: REPORT_KEY,
      frequency: schedule.frequency,
      period_start: period.startDate,
      period_end: period.endDate,
      status: 'skipped',
      error_message: 'No recipients configured',
      sent_at: new Date().toISOString(),
    });
    return { status: 'skipped', message: 'No recipients configured' };
  }

  const existingSuccess = await supabase
    .from('report_email_runs')
    .select('id')
    .eq('schedule_id', schedule.id)
    .eq('period_start', period.startDate)
    .eq('period_end', period.endDate)
    .eq('status', 'success')
    .limit(1)
    .maybeSingle();

  if (existingSuccess.error) {
    throw existingSuccess.error;
  }

  if (existingSuccess.data || schedule.last_sent_for_period === period.endDate) {
    return { status: 'skipped', message: 'Already sent for period' };
  }

  try {
    const rows = await getOperationRows(supabase, period.startDate, period.endDate);
    const summary = getSummary(rows);
    const subject = getSubject(schedule.frequency, period.startDate, period.endDate);
    const csvContent = getCsvContent(rows);
    const csvFilename = `stock-accounting-${schedule.frequency}-${period.startDate}-to-${period.endDate}.csv`;
    const { plainTextBody, htmlBody } = getBodyContent(summary, period.startDate, period.endDate, 'all');

    await sendReportEmail(
      supabaseUrl,
      serviceRoleKey,
      recipients,
      subject,
      htmlBody,
      plainTextBody,
      csvFilename,
      csvContent,
    );

    await supabase.from('report_email_runs').insert({
      schedule_id: schedule.id,
      report_key: REPORT_KEY,
      frequency: schedule.frequency,
      period_start: period.startDate,
      period_end: period.endDate,
      status: 'success',
      sent_at: new Date().toISOString(),
    });

    await supabase
      .from('report_email_schedules')
      .update({ last_sent_for_period: period.endDate, updated_at: new Date().toISOString() })
      .eq('id', schedule.id);

    return { status: 'success', rows: rows.length, recipients: recipients.length };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await supabase.from('report_email_runs').insert({
      schedule_id: schedule.id,
      report_key: REPORT_KEY,
      frequency: schedule.frequency,
      period_start: period.startDate,
      period_end: period.endDate,
      status: 'failed',
      error_message: message,
      sent_at: new Date().toISOString(),
    });
    return { status: 'failed', message };
  }
};

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
    if (!supabaseUrl || !serviceRoleKey) {
      throw new Error('Missing Supabase environment configuration');
    }

    const supabase = createClient(supabaseUrl, serviceRoleKey);
    const requestBody = await req.json();
    const parsed = requestSchema.safeParse(requestBody);
    if (!parsed.success) {
      return new Response(JSON.stringify({
        success: false,
        error: 'Validation failed',
        details: parsed.error.issues.map((i) => ({ field: i.path.join('.'), message: i.message })),
      }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    const isScheduledDispatch = parsed.data.operation === 'scheduled_dispatch';
    const cronToken = req.headers.get('x-cron-token');
    const expectedCronToken = Deno.env.get('STOCK_REPORT_CRON_TOKEN');

    if (isScheduledDispatch && expectedCronToken && cronToken === expectedCronToken) {
      // Authorized by cron token
    } else {
      const authHeader = req.headers.get('Authorization');
      if (!authHeader) {
        return new Response(JSON.stringify({ success: false, error: 'Unauthorized: Missing authorization header' }), {
          status: 401,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }

      const token = authHeader.replace('Bearer ', '');
      const { data: { user }, error: authError } = await supabase.auth.getUser(token);
      if (authError || !user) {
        return new Response(JSON.stringify({ success: false, error: 'Unauthorized: Invalid token' }), {
          status: 401,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }

      const hasAccess = await getRoleAccess(supabase, user.id);
      if (!hasAccess) {
        return new Response(JSON.stringify({ success: false, error: 'Forbidden: Admin access required' }), {
          status: 403,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }
    }

    if (parsed.data.operation === 'manual_send') {
      const result = await handleManualSend(supabase, supabaseUrl, serviceRoleKey, parsed.data);
      return new Response(JSON.stringify(result), {
        status: 200,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }

    const istNow = toIstDate();
    const currentMinutes = getIstMinutes(istNow);
    const dayOfMonth = istNow.getUTCDate();
    const dayOfWeek = istNow.getUTCDay();

    const { data: schedulesData, error: schedulesError } = await supabase
      .from('report_email_schedules')
      .select('id, frequency, recipients, send_time, enabled, last_sent_for_period')
      .eq('report_key', REPORT_KEY)
      .eq('enabled', true);

    if (schedulesError) throw schedulesError;

    const schedules = (schedulesData || []) as Array<{
      id: string;
      frequency: Frequency;
      recipients: string[] | null;
      send_time: string;
      enabled: boolean;
      last_sent_for_period: string | null;
    }>;

    const results: Array<Record<string, unknown>> = [];
    for (const schedule of schedules) {
      const sendMinutes = parseTimeToMinutes(schedule.send_time || '07:00:00');
      if (currentMinutes < sendMinutes) {
        results.push({ scheduleId: schedule.id, frequency: schedule.frequency, status: 'skipped', reason: 'before-send-time' });
        continue;
      }

      let period: { startDate: string; endDate: string } | null = null;
      if (schedule.frequency === 'daily') {
        period = getPreviousDayRange(istNow);
      } else if (schedule.frequency === 'weekly' && dayOfWeek === 1) {
        period = getPreviousWeekRange(istNow);
      } else if (schedule.frequency === 'monthly' && dayOfMonth === 1) {
        period = getPreviousMonthRange(istNow);
      }

      if (!period) {
        results.push({ scheduleId: schedule.id, frequency: schedule.frequency, status: 'skipped', reason: 'not-due-today' });
        continue;
      }

      const result = await runSchedule(supabase, supabaseUrl, serviceRoleKey, schedule, period);
      results.push({ scheduleId: schedule.id, frequency: schedule.frequency, ...result, period });
    }

    return new Response(JSON.stringify({ success: true, dispatchedAt: new Date().toISOString(), results }), {
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('send-stock-accounting-report failed:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error instanceof Error ? error.message : String(error),
    }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
});
