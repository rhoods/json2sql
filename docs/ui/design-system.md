# Design System Document: Precision Editorial for Data Engineering

## 1. Overview & Creative North Star: "The Architectural Logic"
The North Star for this design system is **The Architectural Logic**. Unlike consumer apps that prioritize whitespace, this system treats data as the primary inhabitant of the interface. It rejects the "standard web app" aesthetic in favor of a high-density, professional editorial layout—reminiscent of a masterfully printed technical manual or a sophisticated cockpit.

We achieve "Premium Density" not through clutter, but through intentional hierarchy. By breaking the traditional rigid grid with subtle tonal layering and high-contrast monospace accents, we create a tool that feels like an extension of a developer’s brain: fast, precise, and devoid of visual "fluff."

## 2. Colors: Tonal Depth & The "No-Line" Rule
This system moves away from flat, monotonous charcoal. Instead, it utilizes a spectrum of depth to guide the eye.

### Surface Hierarchy & Nesting
Traditional borders are a relic of low-resolution screens. In this system, we use **The "No-Line" Rule**: logic is defined by background shifts, not strokes.
*   **Root Level:** `surface` (#131313) — The foundation of the application window.
*   **Primary Workspaces:** `surface_container_low` (#1B1B1C) — Main editor backgrounds.
*   **Active Overlays:** `surface_container_high` (#2A2A2A) — Tool windows and sidebars.
*   **Interactive Inputs:** `surface_container_highest` (#353535) — Fields and dropdowns.

### The "Glass & Gradient" Rule
To elevate the UI beyond a standard IDE, primary actions and floating states utilize "Visual Soul":
*   **Signature Gradients:** Main CTAs (Primary) must use a subtle linear gradient from `primary` (#99CBFF) to `primary_container` (#007BC4) at a 135° angle. This adds a "machined metal" feel rather than a flat plastic look.
*   **Glassmorphism:** Context menus and floating palettes use `surface_container` at 80% opacity with a `20px` backdrop-blur. This ensures the complex data underneath remains "felt" but not distracting.

### Accents (Semantic Tokens)
*   **Primary (Action):** `primary` (#99CBFF) — For the flow of intent.
*   **Secondary (Success):** `secondary` (#4EDEA3) — For active connections and successful queries.
*   **Tertiary (Warning):** `tertiary` (#FFB95F) — For data truncation or performance alerts.
*   **Error:** `error` (#FFB4AB) — For syntax violations and connection failures.

## 3. Typography: The Monospaced Authority
Typography is our primary tool for hierarchy. We pair the humanist clarity of **Inter** with the structural rigidity of **JetBrains Mono**.

*   **Editorial Headlines:** Use `headline-sm` (Inter, 1.5rem) for main view titles. Letter spacing should be tightened (-0.02em) to feel authoritative.
*   **Data Labels:** Use `label-sm` (Inter, 0.6875rem, All Caps) with 0.05em tracking for metadata.
*   **The Code Core:** All SQL, JSON, and schema names must use `JetBrains Mono`. This signals to the user exactly what is "data" vs. what is "interface."
*   **Visual Balance:** Body text is kept at `body-md` (0.875rem) to maintain density while ensuring legibility during long debugging sessions.

## 4. Elevation & Depth: Tonal Layering
We do not use drop shadows to indicate height; we use light.

*   **The Layering Principle:** A tree view (Sidebar) should sit on `surface_container_low`, while the SQL Editor sits on `surface_container_lowest`. The physical "recession" of the editor creates a focus trap for the user.
*   **Ambient Shadows:** For floating modals, use a "Tinted Glow" rather than a black shadow. Use `on_surface` at 6% opacity with a `32px` blur and `0px` offset.
*   **The "Ghost Border" Fallback:** If high-contrast separation is required (e.g., between two dark code panes), use the `outline_variant` (#404752) at 40% opacity. Never use a 100% opaque border.

## 5. Components: Engineered for Performance

### Buttons
*   **Primary:** Gradient fill (`primary` to `primary_container`), `roundness-sm` (2px), 0.5px "Ghost Border" of `primary_fixed_dim` at 20% for a "beveled" edge.
*   **Ghost (Secondary):** No background. `primary` text. `surface_bright` background on hover.

### Form Inputs & Tree Views
*   **Inputs:** Use `surface_container_highest` with a bottom-only "Ghost Border." On focus, the border expands to 2px using the `primary` blue.
*   **Tree View:** High-density padding (4px vertical). Active nodes use `secondary_container` (#00A572) at 20% opacity with a 2px vertical "accent bar" on the left edge.

### Code Blocks & Syntax Highlighting
*   **The Editor:** Background: `surface_container_lowest`.
*   **Syntax:** JSON keys in `primary`, SQL keywords in `secondary`, Strings in `tertiary`, and Braces in `on_surface_variant`.

### Progress Bars
*   **Track:** `surface_container_highest`.
*   **Indicator:** A 2-stop gradient of `secondary` to `secondary_fixed`. No rounded corners on the indicator—keep it "Brutalist" and sharp.

## 6. Do’s and Don’ts

### Do:
*   **DO** use "Intentional Asymmetry." For example, right-align metadata labels while left-aligning primary data to create a clear visual gutter.
*   **DO** leverage `surface_container` tiers to group related items instead of using boxes or lines.
*   **DO** use `JetBrains Mono` for any text that can be copied/pasted into a terminal.

### Don’t:
*   **DON’T** use `roundness-xl` or `full`. Professional tools feel more stable with `sm` (2px) or `md` (6px) corners.
*   **DON’T** use pure black (#000) or pure white (#FFF). Use the `surface` and `on_surface` tokens to maintain eye comfort.
*   **DON’T** use standard "Drop Shadows." If an element needs to pop, increase its `surface_container` tier.