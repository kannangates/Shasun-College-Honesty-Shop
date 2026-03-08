# Shasun College Honesty Shop 🏫

A modern honesty shop management system built specifically for **Shasun College**. This application promotes integrity and trust by allowing students to purchase items on an honor system, tracking their honesty points and badges.

_No Cameras 📷 | No Cashiers 💳 | Just Character 🫡_

## ✨ Features

### Core Features

- **Self-service shopping**: Students browse and purchase products without a cashier
- **Honesty-based payments**: Multiple payment options including Pay Now and Pay Later
- **Points & Gamification**: Earn honesty points for timely payments
- **Badge System**: Unlock achievement badges based on behavior
- **Real-time Dashboard**: Live statistics on sales, top students, and departments
- **Reorder Flow**: Quickly reorder items from previous orders with Pay Now / Pay Later options

### Admin Features

- **Inventory Management**: Track shelf and warehouse stock levels
- **Student Management**: Manage student accounts and points
- **Order Management**: View and manage all orders
- **Payment Reports**: Generate detailed payment reports
- **Stock Operations**: Daily stock reconciliation and auditing
- **Audit Logs**: Complete audit trail for admin actions
- **Announcements**: Send targeted or department-wide notifications

### Security Features

- **Role-based Access Control**: Admin, Developer, Student, and Teacher roles
- **Row Level Security (RLS)**: Database-level security policies
- **Two-Factor Authentication (2FA)**: Optional MFA with login requirement toggle
- **Session Management**: Secure session handling with automatic timeouts
- **PII Protection**: MFA-protected access to sensitive student information

## 🛠️ Technology Stack

- **Frontend**: React 18, TypeScript, Tailwind CSS, Vite
- **Backend**: Supabase (PostgreSQL, Edge Functions, Authentication)
- **UI Components**: shadcn/ui, Radix UI
- **State Management**: React Query (TanStack Query)
- **Charts**: Recharts
- **Forms**: React Hook Form with Zod validation

## 📋 Prerequisites

- Node.js 18+
- npm or yarn
- Supabase account and project

## 🚀 Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd shasun-honesty-shop
npm install
```

### 2. Environment Configuration

Create a `.env` file in the root directory:

```env
# Supabase Configuration (Required)
VITE_SUPABASE_PROJECT_ID="your-project-id"
VITE_SUPABASE_PUBLISHABLE_KEY="your-anon-key"
VITE_SUPABASE_URL="https://your-project-id.supabase.co"
```

### 3. Supabase Setup

1. **Create a Supabase project** at [supabase.com](https://supabase.com)

2. **Run database migrations**: The migrations in `supabase/migrations/` will set up:
   - User tables with role-based access
   - Product and inventory tables
   - Order management tables
   - Gamification tables (badges, points)
   - RLS policies for security

3. **Configure Authentication**:
   - Go to Authentication → Settings
   - Disable "Confirm email" for faster testing (optional)
   - Set Site URL to your deployment URL

4. **Set up Edge Functions secrets** in Supabase Dashboard → Settings → Functions:
   - `SUPABASE_SERVICE_ROLE_KEY` (Required)
   - `HCAPTCHA_SECRET_KEY` (Optional, for captcha)
   - `GMAIL_*` secrets (Optional, for email notifications)

5. **Configure 2FA (Optional)**:
   - 2FA functions are automatically deployed with the edge functions
   - No additional configuration required
   - Users can enable 2FA individually in their Settings

### 4. Create Initial Admin User

1. Sign up through the application with your college email
2. In Supabase Dashboard → SQL Editor, run:

```sql
UPDATE public.users SET role = 'admin' WHERE student_id = 'YOUR_STUDENT_ID';

INSERT INTO public.user_roles (user_id, role)
SELECT id, 'admin' FROM public.users WHERE student_id = 'YOUR_STUDENT_ID';
```

### 5. Run Development Server

```bash
npm run dev
```

The application will be available at `http://localhost:5173`

## 📁 Project Structure

```
├── src/
│   ├── components/          # React components
│   │   ├── admin/           # Admin-specific components
│   │   ├── auth/            # Authentication components
│   │   ├── checkout/        # Reorder checkout flow
│   │   ├── common/          # Shared components
│   │   ├── dashboard/       # Dashboard components
│   │   └── ui/              # UI primitives (shadcn)
│   ├── contexts/            # React contexts (Auth, Product)
│   ├── features/            # Feature modules (gamification)
│   ├── hooks/               # Custom React hooks
│   ├── pages/               # Page components
│   │   └── admin/           # Admin pages
│   ├── services/            # API services
│   ├── types/               # TypeScript types (see Types section below)
│   └── utils/               # Utility functions
├── server/                  # Express server for API routes
├── supabase/
│   ├── functions/           # Edge functions (auth, MFA, orders, etc.)
│   └── migrations/          # Database migrations
├── docs/                    # Extended documentation
│   ├── getting-started.md
│   ├── app-functions-guide.md
│   ├── api-documentation.md
│   ├── user-guides/user-guide.md
│   └── advanced-analytics-plan.md
└── public/                  # Static assets
```

## 🔐 Security & Authentication

### Two-Factor Authentication (2FA)

- **Setup**: Settings → Security Settings → Enable 2FA → Scan QR with authenticator app
- **Service Name**: Shows as "Shasun College Honesty Shop" in authenticator apps
- **Login Requirement Toggle**: Choose whether 2FA is required for every login
- **PII Protection**: Admin access to sensitive student info requires MFA verification

### MFA Edge Functions

- `supabase/functions/mfa-setup/` — Generate MFA secrets and QR codes
- `supabase/functions/mfa-verify/` — Verify MFA tokens and enable MFA
- `supabase/functions/mfa-status/` — Check MFA status
- `supabase/functions/mfa-disable/` — Disable MFA
- `supabase/functions/mfa-verify-session/` — Verify MFA for PII access

### User Roles

| Role          | Permissions                                                                           |
| ------------- | ------------------------------------------------------------------------------------- |
| **Admin**     | Full access to all features including user management, inventory, and system settings |
| **Developer** | Same as Admin, plus access to developer tools and system diagnostics                  |
| **Teacher**   | Standard access with ability to view reports                                          |
| **Student**   | Browse products, make purchases, view own orders and points                           |

## 🎮 Gamification System

### Points System

| Action                      | Points |
| --------------------------- | ------ |
| Immediate Payment           | +10    |
| Payment within 30 hours     | +8     |
| Payment within 48 hours     | +5     |
| Payment within 72 hours     | +2     |
| Late Payment (after 72h)    | -5     |

### Badges

Badges are awarded based on achievements such as first purchase, consistent timely payments, department rankings, and point milestones.

## 📝 Types System

All TypeScript types are organized by domain in `src/types/`:

| File            | Contents                                                    |
| --------------- | ----------------------------------------------------------- |
| `index.ts`      | Central re-exports for convenience                          |
| `auth.ts`       | `UserProfile`, `AuthSession`, `LoginResult`, etc.           |
| `database.ts`   | `User`, `Product`, `Order`, `Badge`, database entity types  |
| `common.ts`     | `CartItem`, `DashboardStats`, UI component props            |
| `api.ts`        | Edge function request/response types                        |
| `hooks.ts`      | `UseCartReturn`, `UseAuthReturn`, hook return types         |
| `constants.ts`  | `USER_ROLES`, `PAYMENT_STATUS`, type-safe constants         |
| `branding.ts`   | Whitelabel branding configuration types                     |
| `supabase.ts`   | Auto-generated Supabase database types                      |

**Best practices**: Use `import type` for all type imports. Extend base types instead of duplicating. Use type-safe constants from `constants.ts`.

## 🖥️ Express Server

The Express server (`server/`) handles API routes for the Vite React application. MFA functionality has been moved to Supabase Edge Functions.

- `server/index.js` — Main Express server with Vite middleware
- `server/lib/supabase.js` — Supabase admin client and auth helpers

All endpoints require authentication via Bearer token in the Authorization header.

## 🚢 Deployment

### Build for Production

```bash
npm run build
```

### Deploy to Netlify

1. Connect your repository to Netlify
2. Set build command: `npm run build`
3. Set publish directory: `dist`
4. Add environment variables in Netlify dashboard

### Deploy to Vercel

1. Import your repository to Vercel
2. Framework preset: Vite
3. Add environment variables in project settings

### Edge Functions Deployment

```bash
supabase link --project-ref your-project-id
supabase secrets set SECRET_NAME=value
```

Edge functions in `supabase/functions/` are automatically deployed when you push to your Supabase project.

## 🔧 Configuration

- **Email Domain**: `@shasuncollege.edu.in` — configured in `src/services/authService.ts`
- **Points Config**: Stored in `points_config` table, modifiable via admin panel
- **Session Timeout**: Configured in `src/config.ts`

## 📊 Database Schema

Key tables: `users`, `user_roles`, `user_mfa`, `products`, `orders`, `order_items`, `badges`, `user_badges`, `daily_stock_operations`, `admin_audit_log`, `notifications`, `notification_reads`, `points_config`, `points_log`, `gamification_rules`

## 🐛 Troubleshooting

| Issue | Solution |
| ----- | -------- |
| Failed to load students | Ensure edge functions are deployed and secrets configured |
| Authentication errors | Check Supabase auth settings and email domain |
| Permission denied | Verify RLS policies and user roles |
| 2FA QR code not loading | Ensure MFA edge functions are deployed |
| 2FA verification fails | Check authenticator app time is synchronized |
| MFA not set up error | User needs to complete 2FA setup first |

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## 👥 Support

For technical support or questions, contact the IT department at Shasun College.

---

**Built with ❤️ for Shasun College**
