import { useEffect, useRef } from "react";

const FOCUSABLE =
  'a[href], button:not([disabled]), textarea:not([disabled]), input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])';

/**
 * Traps keyboard focus inside a modal container and restores focus to the
 * trigger element when the modal closes.
 *
 * Usage:
 *   const containerRef = useFocusTrap(isOpen);
 *   <div ref={containerRef}>…modal content…</div>
 */
export function useFocusTrap(isOpen: boolean) {
  const containerRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<Element | null>(null);

  // Capture the element that had focus when the modal opened so we can
  // return to it on close.
  useEffect(() => {
    if (isOpen) {
      triggerRef.current = document.activeElement;
    }
  }, [isOpen]);

  // Move initial focus into the modal (first focusable element).
  useEffect(() => {
    if (!isOpen || !containerRef.current) return;
    const first = containerRef.current.querySelector<HTMLElement>(FOCUSABLE);
    // Small delay lets the DOM settle after render.
    const id = requestAnimationFrame(() => first?.focus());
    return () => cancelAnimationFrame(id);
  }, [isOpen]);

  // Trap Tab / Shift+Tab inside the container.
  useEffect(() => {
    if (!isOpen) return;
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key !== "Tab" || !containerRef.current) return;
      const focusable = Array.from(
        containerRef.current.querySelectorAll<HTMLElement>(FOCUSABLE),
      );
      if (focusable.length === 0) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];

      if (e.shiftKey) {
        if (document.activeElement === first) {
          e.preventDefault();
          last.focus();
        }
      } else {
        if (document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [isOpen]);

  // Restore focus to the trigger element when the modal closes.
  useEffect(() => {
    if (isOpen) return;
    const el = triggerRef.current;
    if (el && el instanceof HTMLElement) {
      el.focus();
      triggerRef.current = null;
    }
  }, [isOpen]);

  return containerRef;
}
