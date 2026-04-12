// @ts-nocheck
import { ScrollViewStyleReset } from "expo-router/html";
import type { PropsWithChildren } from "react";

export default function Root({ children }: PropsWithChildren) {
  return (
    <html lang="en" style={{ height: "100%" }}>
      <head>
        <meta charSet="utf-8" />
        <meta httpEquiv="X-UA-Compatible" content="IE=edge" />
        <meta
          name="viewport"
          content="width=device-width, initial-scale=1, shrink-to-fit=no"
        />
        {/*
          Disable body scrolling on web to make ScrollView components work correctly.
          If you want to enable scrolling, remove `ScrollViewStyleReset` and
          set `overflow: auto` on the body style below.
        */}
        <ScrollViewStyleReset />
        <style
          dangerouslySetInnerHTML={{
            __html: `
              body > div:first-child { position: fixed !important; top: 0; left: 0; right: 0; bottom: 0; }
              [role="tablist"] [role="tab"] * { overflow: visible !important; }
              [role="heading"], [role="heading"] * { overflow: visible !important; }
              /* Global Layout Standard: rail background + no horizontal scroll */
              html, body { overflow-x: hidden; }
              /*
               * Constrain RNW Modal portals to the app shell width (430px).
               * React Native Web's <Modal> renders via createPortal into document.body
               * with position:fixed inset:0, causing overlays to span the full viewport.
               * This rule caps the dialog surface and centres it, matching the app shell.
               * On mobile viewports (≤430px) max-width has no effect → no regression.
               */
              [aria-modal="true"] {
                max-width: 430px !important;
                margin-left: auto !important;
                margin-right: auto !important;
              }
            `,
          }}
        />
      </head>
      <body
        style={{
          margin: 0,
          height: "100%",
          overflow: "hidden",
          display: "flex",
          flexDirection: "column",
          /* Rail background visible behind the centred app shell */
          backgroundColor: "#E8E4DF",
        }}
      >
        {children}
      </body>
    </html>
  );
}
