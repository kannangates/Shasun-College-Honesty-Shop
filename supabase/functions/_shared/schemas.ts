import { z } from 'https://deno.land/x/zod@v3.22.4/mod.ts';

// Common field validations - ORDERED BY DEPENDENCY
export const studentIdSchema = z.string()
  .min(1, 'Student ID is required')
  .max(50, 'Student ID must be 50 characters or less')
  .trim()
  .regex(/^[A-Za-z0-9-_]+$/, 'Student ID must contain only letters, numbers, hyphens, and underscores');

export const nameSchema = z.string()
  .min(1, 'Name is required')
  .max(100, 'Name must be 100 characters or less')
  .trim();

export const emailSchema = z.string()
  .email('Invalid email format')
  .max(255, 'Email must be 255 characters or less')
  .toLowerCase();

export const passwordSchema = z.string()
  .min(8, 'Password must be at least 8 characters')
  .max(128, 'Password must be 128 characters or less');

export const roleSchema = z.enum(['student', 'teacher', 'admin', 'developer'])
  .default('student');

export const shiftSchema = z.enum(['Morning (1st Shift)', 'Evening (2nd Shift)', 'Full Shift'])
  .default('Morning (1st Shift)');

// Define these BEFORE they're used in other schemas
export const departmentSchema = z.string()
  .min(1, 'Department is required')
  .max(100, 'Department must be 100 characters or less')
  .trim();

export const reasonSchema = z.string()
  .min(1, 'Reason is required')
  .max(500, 'Reason must be 500 characters or less')
  .trim();

export const mfaTokenSchema = z.string()
  .min(6, 'MFA token must be at least 6 digits')
  .max(10, 'MFA token must be 10 digits or less')
  .regex(/^[0-9]+$/, 'MFA token must be numeric');

export const mobileNumberSchema = z.string()
  .max(20, 'Mobile number must be 20 characters or less')
  .regex(/^[0-9+\-() ]*$/, 'Invalid mobile number format')
  .nullable()
  .optional();

export const uuidSchema = z.string()
  .uuid('Invalid UUID format');

// Now define schemas that use the above
export const authLoginSchema = z.object({
  studentId: studentIdSchema,
  password: passwordSchema
});

export const authSignupSchema = z.object({
  studentId: studentIdSchema,
  name: nameSchema,
  department: departmentSchema,
  email: emailSchema,
  password: passwordSchema,
  role: roleSchema,
  shift: shiftSchema,
  points: z.number().int().min(0).max(100000).default(100),
  userMetadata: z.record(z.unknown()).optional().default({}),
  skipCaptcha: z.boolean().optional().default(false)
});

// Public signup schema
export const publicSignupSchema = z.object({
  studentId: studentIdSchema,
  name: nameSchema,
  department: departmentSchema,
  email: emailSchema,
  password: passwordSchema,
  shift: z.enum(['Morning (1st Shift)', 'Evening (2nd Shift)', 'Full Shift']).default('Morning (1st Shift)'),
  points: z.number().int().min(0).max(100000).default(100),
  userMetadata: z.record(z.unknown()).optional().default({})
});

// User update schema
export const userUpdateSchema = z.object({
  id: uuidSchema,
  name: nameSchema.optional(),
  email: emailSchema.optional(),
  department: departmentSchema.optional(),
  mobile_number: mobileNumberSchema,
  status: z.enum(['active', 'inactive', 'suspended']).optional()
});

// Order update schema
export const orderUpdateSchema = z.object({
  id: uuidSchema,
  payment_status: z.enum(['paid', 'unpaid', 'pending', 'refunded']).optional(),
  order_status: z.enum(['pending', 'processing', 'completed', 'cancelled']).optional(),
  transaction_id: z.string().max(255).nullable().optional(),
  payment_mode: z.string().max(50).nullable().optional(),
  paid_at: z.string().datetime().nullable().optional(),
  updated_by: uuidSchema.optional()
});

// Stock operation schema
export const stockOperationSchema = z.object({
  operation: z.enum(['restock_warehouse', 'restock_shelf', 'adjust_shelf_stock', 'get_stock_status']),
  productId: uuidSchema,
  quantity: z.number().int().min(-10000).max(10000),
  source: z.enum(['Product Inventory', 'Order Management', 'Checkout']).optional()
});

// User management operation schema
export const userManagementSchema = z.discriminatedUnion('operation', [
  z.object({
    operation: z.literal('fetch_user_summary')
  }),
  z.object({
    operation: z.literal('fetch_user_details'),
    targetUserId: uuidSchema,
    reason: reasonSchema,
    mfaToken: mfaTokenSchema
  }),
  z.object({
    operation: z.literal('fetch_leaderboard')
  }),
  z.object({
    operation: z.literal('update_user'),
    id: uuidSchema,
    name: nameSchema.optional(),
    email: emailSchema.optional(),
    department: departmentSchema.optional(),
    mobile_number: mobileNumberSchema,
    status: z.enum(['active', 'inactive', 'suspended']).optional(),
    role: z.enum(['student', 'teacher', 'admin', 'developer']).optional(),
    shift: shiftSchema.optional()
  }),
  z.object({
    operation: z.literal('get_stats')
  }),
  z.object({
    operation: z.literal('update_last_signin'),
    userId: uuidSchema
  })
]);

// Order management operation schema
export const orderManagementSchema = z.discriminatedUnion('operation', [
  z.object({
    operation: z.literal('fetch_orders')
  }),
  z.object({
    operation: z.literal('get_stats')
  }),
  z.object({
    operation: z.literal('update_order'),
    id: uuidSchema,
    payment_status: z.enum(['paid', 'unpaid', 'pending', 'refunded']).optional(),
    order_status: z.enum(['pending', 'processing', 'completed', 'cancelled']).optional(),
    transaction_id: z.string().max(255).nullable().optional(),
    payment_mode: z.string().max(50).nullable().optional(),
    paid_at: z.string().datetime().nullable().optional(),
    updated_by: uuidSchema.optional()
  })
]);

// New password schema
export const newPasswordSchema = z.string()
  .min(8, 'New password must be at least 8 characters')
  .max(128, 'New password must be 128 characters or less');

// Points adjustment
export const pointsSchema = z.number()
  .int('Points must be an integer')
  .min(-100000, 'Points cannot decrease by more than 100000')
  .max(100000, 'Points cannot increase by more than 100000');

// Token for captcha
export const tokenSchema = z.string()
  .min(1, 'Token is required')
  .max(10000, 'Token is too long');

// Captcha secret key
export const captchaSecretSchema = z.string()
  .min(1, 'Secret key is required')
  .max(500, 'Secret key is too long');

// Configuration object
export const configSchema = z.record(z.unknown())
  .refine(
    (obj) => Object.keys(obj).length > 0,
    { message: 'Config object cannot be empty' }
  );

// HTML email body
export const htmlBodySchema = z.string()
  .min(1, 'Email body is required')
  .max(50000, 'Email body must be 50000 characters or less')
  .trim();

// Plain text email body
export const plainTextBodySchema = z.string()
  .max(50000, 'Plain text body must be 50000 characters or less')
  .optional();

// Email CC list
export const emailCCSchema = z.array(emailSchema).optional();

// Email attachments
export const emailAttachmentSchema = z.object({
  filename: z.string()
    .min(1, 'Attachment filename is required')
    .max(255, 'Attachment filename must be 255 characters or less'),
  contentType: z.string()
    .min(1, 'Attachment content type is required')
    .max(100, 'Attachment content type must be 100 characters or less'),
  base64Content: z.string()
    .min(1, 'Attachment content is required')
    .max(10_000_000, 'Attachment content is too large')
});

export const emailAttachmentsSchema = z.array(emailAttachmentSchema).max(10).optional();

// Inventory operation row
export const inventoryOperationSchema = z.object({
  product_id: uuidSchema,
  opening_stock: z.number().int().min(0),
  additional_stock: z.number().int().min(0),
  actual_closing_stock: z.number().int().min(0),
  estimated_closing_stock: z.number().int().min(0),
  stolen_stock: z.number().int().min(0).default(0),
  wastage_stock: z.number().int().min(0).default(0),
  sales: z.number().int().min(0).default(0),
  order_count: z.number().int().min(0).default(0)
});

// Badge award request
export const badgeAwardRequestSchema = z.object({
  userId: uuidSchema,
  orderId: uuidSchema.optional()
});

// Update points request
export const updatePointsSchema = z.object({
  studentId: studentIdSchema,
  points: pointsSchema,
  reason: reasonSchema
});

// Captcha verification request
export const captchaVerificationSchema = z.object({
  token: tokenSchema,
  secretKey: captchaSecretSchema
});

// Forgot password request
export const forgotPasswordSchema = z.object({
  studentId: studentIdSchema
});

// Admin reset password request
export const adminResetPasswordSchema = z.object({
  userId: uuidSchema,
  newPassword: newPasswordSchema
});

// Update user role request
export const updateRoleSchema = z.object({
  userId: uuidSchema,
  newRole: z.enum(['student', 'teacher', 'admin', 'developer'])
});

// Email request schema
export const emailRequestSchema = z.object({
  to: emailSchema,
  subject: z.string()
    .min(1, 'Subject is required')
    .max(255, 'Subject must be 255 characters or less')
    .trim(),
  htmlBody: htmlBodySchema,
  plainTextBody: plainTextBodySchema,
  cc: emailCCSchema,
  replyTo: emailSchema.optional(),
  fromName: z.string()
    .max(100, 'From name must be 100 characters or less')
    .default('Shasun Honesty Shop'),
  fromEmail: emailSchema.optional(),
  attachments: emailAttachmentsSchema
});

// Daily inventory save request
export const dailyInventorySaveSchema = z.object({
  data: z.array(inventoryOperationSchema)
});
