import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import { FeedbackFooter } from "@/components/feedback/FeedbackFooter";
import { CookieBanner } from "@/components/consent/CookieBanner";
import { PerformanceTelemetry } from "@/components/consent/PerformanceTelemetry";
import { LanguageProvider } from "@/lib/LanguageContext";
import { CookieConsentProvider } from "@/lib/CookieConsentContext";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "ArasiyalAayvu | அரசியல்ஆய்வு",
  description: "Tamil Nadu election awareness through verified and verifiable data",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    // App-shell layout: body is exactly one viewport tall and never scrolls.
    // The middle region scrolls; each page's sticky top bar sticks to the top
    // of that scroll container, and the feedback footer is pinned at the
    // bottom. h-svh (small viewport height) avoids mobile-browser URL-bar
    // resize jank; fallback to h-screen for older engines.
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body
        suppressHydrationWarning
        className="h-screen h-svh overflow-hidden flex flex-col"
      >
        <CookieConsentProvider>
          <LanguageProvider>
            <div className="flex-1 overflow-y-auto">{children}</div>
            <div className="flex-shrink-0">
              <FeedbackFooter />
            </div>
            <CookieBanner />
          </LanguageProvider>
          {/* Only mounts when performance-cookie consent = true. */}
          <PerformanceTelemetry />
        </CookieConsentProvider>
      </body>
    </html>
  );
}
