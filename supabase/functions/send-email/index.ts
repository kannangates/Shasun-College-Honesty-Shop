import { serve } from "https://deno.land/std@0.190.0/http/server.ts";
import { emailRequestSchema } from '../_shared/schemas.ts';

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

interface EmailRequest {
  to: string;
  subject: string;
  htmlBody: string;
  plainTextBody?: string;
  cc?: string[];
  replyTo?: string;
  fromName?: string;
  fromEmail?: string;
  attachments?: Array<{
    filename: string;
    contentType: string;
    base64Content: string;
  }>;
}

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

const handler = async (req: Request): Promise<Response> => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const requestBody = await req.json();

    // Validate input with Zod schema
    const validationResult = emailRequestSchema.safeParse(requestBody);
    if (!validationResult.success) {
      type ZodIssue = { path: Array<string | number>; message: string };
      const issues = (validationResult as unknown as { error: { issues: ZodIssue[] } }).error.issues as ZodIssue[];
      return new Response(
        JSON.stringify({
          error: 'Validation failed',
          details: issues.map(e => ({ field: e.path.join('.'), message: e.message }))
        }),
        { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    const { to, subject, htmlBody, plainTextBody, cc, replyTo, fromName, fromEmail, attachments } = validationResult.data;

    // Get Gmail credentials from Supabase secrets
    const gmailUser = Deno.env.get('GMAIL_USER');
    const gmailClientId = Deno.env.get('GMAIL_CLIENT_ID');
    const gmailClientSecret = Deno.env.get('GMAIL_CLIENT_SECRET');
    const gmailRefreshToken = Deno.env.get('GMAIL_REFRESH_TOKEN');

    // Override recipient for error reports to specific email
    const finalTo = subject.includes('Error report') ? 'honesty@shasuncollege.edu.in' : to;

    if (!gmailUser || !gmailClientId || !gmailClientSecret || !gmailRefreshToken) {
      throw new Error('Gmail credentials not configured');
    }

    // Get access token from refresh token
    const tokenResponse = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        client_id: gmailClientId,
        client_secret: gmailClientSecret,
        refresh_token: gmailRefreshToken,
        grant_type: 'refresh_token',
      }),
    });

    const tokenData = await tokenResponse.json();
    if (!tokenData.access_token) {
      throw new Error('Failed to get access token');
    }

    // Construct email
    const fromAddress = fromEmail || gmailUser;
    const hasAttachments = Boolean(attachments && attachments.length > 0);
    const emailHeaders = [
      `From: ${fromName} <${fromAddress}>`,
      `To: ${finalTo}`,
      cc && cc.length > 0 ? `Cc: ${cc.join(', ')}` : '',
      replyTo ? `Reply-To: ${replyTo}` : '',
      `Subject: ${subject}`,
      'MIME-Version: 1.0',
      hasAttachments
        ? 'Content-Type: multipart/mixed; boundary="mixedBoundary456"'
        : 'Content-Type: multipart/alternative; boundary="altBoundary123"'
    ].filter(Boolean).join('\r\n');

    const alternativeBody = hasAttachments
      ? [
        '--mixedBoundary456',
        'Content-Type: multipart/alternative; boundary="altBoundary123"',
        '',
        '--altBoundary123',
        'Content-Type: text/plain; charset=UTF-8',
        '',
        plainTextBody || htmlBody.replace(/<[^>]*>/g, ''),
        '',
        '--altBoundary123',
        'Content-Type: text/html; charset=UTF-8',
        '',
        htmlBody,
        '',
        '--altBoundary123--',
        ''
      ].join('\r\n')
      : [
        '--altBoundary123',
        'Content-Type: text/plain; charset=UTF-8',
        '',
        plainTextBody || htmlBody.replace(/<[^>]*>/g, ''),
        '',
        '--altBoundary123',
        'Content-Type: text/html; charset=UTF-8',
        '',
        htmlBody,
        '',
        '--altBoundary123--'
      ].join('\r\n');

    const attachmentBlocks = (attachments || []).map((attachment) => [
      '--mixedBoundary456',
      `Content-Type: ${attachment.contentType}; name="${attachment.filename}"`,
      'Content-Transfer-Encoding: base64',
      `Content-Disposition: attachment; filename="${attachment.filename}"`,
      '',
      attachment.base64Content,
      ''
    ].join('\r\n')).join('\r\n');

    const emailBody = hasAttachments
      ? ['', alternativeBody, attachmentBlocks, '--mixedBoundary456--'].join('\r\n')
      : ['', alternativeBody].join('\r\n');

    const rawEmail = emailHeaders + emailBody;
    const encodedEmail = toBase64Utf8(rawEmail).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

    // Send email via Gmail API
    const gmailResponse = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${tokenData.access_token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        raw: encodedEmail,
      }),
    });

    const gmailResult = await gmailResponse.json();

    if (!gmailResponse.ok) {
      throw new Error(`Gmail API error: ${gmailResult.error?.message || 'Unknown error'}`);
    }

    console.log('Email sent successfully:', gmailResult);

    return new Response(JSON.stringify({
      success: true,
      messageId: gmailResult.id,
      message: 'Email sent successfully'
    }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders,
      },
    });
  } catch (error: unknown) {
    console.error("Error in send-email function:", error);
    return new Response(
      JSON.stringify({ 
        success: false,
        error: error instanceof Error ? error.message : String(error)
      }),
      {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders },
      }
    );
  }
};

serve(handler);
