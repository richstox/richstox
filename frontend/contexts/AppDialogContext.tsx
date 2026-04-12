/**
 * AppDialogContext – shared in-app replacement for browser-native
 * alert / confirm / prompt dialogs.
 *
 * Uses React Native `<Modal>` which is constrained to the 430 px app
 * shell via the global `[aria-modal="true"]` CSS rule in +html.tsx.
 *
 * Usage:
 *   const dialog = useAppDialog();
 *   await dialog.alert('Title', 'Message');
 *   const ok = await dialog.confirm('Title', 'Are you sure?');
 *   const val = await dialog.prompt('Title', 'Enter value');
 */

import React, { createContext, useCallback, useContext, useRef, useState } from 'react';
import {
  Modal,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from 'react-native';

// ─── Types ──────────────────────────────────────────────────────────────────

type DialogType = 'alert' | 'confirm' | 'prompt';

interface DialogConfig {
  type: DialogType;
  title: string;
  message: string;
  /** Label for the primary (OK / destructive) button. */
  confirmLabel?: string;
  /** Style hint for the primary button. */
  confirmStyle?: 'default' | 'destructive';
  /** Default value for prompt input. */
  defaultValue?: string;
}

interface DialogState extends DialogConfig {
  resolve: (value: boolean | string | null) => void;
}

interface AppDialogAPI {
  /** Show an informational dialog with a single "OK" button. */
  alert: (title: string, message?: string) => Promise<void>;
  /**
   * Show a confirmation dialog with OK / Cancel.
   * Returns `true` if the user pressed OK, `false` otherwise.
   */
  confirm: (
    title: string,
    message: string,
    opts?: { confirmLabel?: string; confirmStyle?: 'default' | 'destructive' },
  ) => Promise<boolean>;
  /**
   * Show a text-input dialog.
   * Returns the entered string, or `null` if the user cancelled.
   */
  prompt: (title: string, message?: string, defaultValue?: string) => Promise<string | null>;
}

const noop: AppDialogAPI = {
  alert: async () => {},
  confirm: async () => false,
  prompt: async () => null,
};

const AppDialogContext = createContext<AppDialogAPI>(noop);

// ─── Provider ───────────────────────────────────────────────────────────────

export function AppDialogProvider({ children }: { children: React.ReactNode }) {
  const [dialog, setDialog] = useState<DialogState | null>(null);
  const [promptValue, setPromptValue] = useState('');
  // Queue additional dialogs so callers never lose a request.
  const queue = useRef<DialogState[]>([]);

  const showNext = useCallback(() => {
    if (queue.current.length > 0) {
      const next = queue.current.shift()!;
      setPromptValue(next.defaultValue ?? '');
      setDialog(next);
    }
  }, []);

  const enqueue = useCallback(
    (cfg: DialogConfig): Promise<any> =>
      new Promise((resolve) => {
        const state: DialogState = { ...cfg, resolve };
        if (dialog) {
          queue.current.push(state);
        } else {
          setPromptValue(cfg.defaultValue ?? '');
          setDialog(state);
        }
      }),
    [dialog],
  );

  const dismiss = useCallback(
    (value: boolean | string | null) => {
      if (dialog) {
        dialog.resolve(value);
        setDialog(null);
        // Show next queued dialog (if any) after a micro-tick so the
        // current modal can animate out first.
        setTimeout(showNext, 0);
      }
    },
    [dialog, showNext],
  );

  const api = React.useMemo<AppDialogAPI>(
    () => ({
      alert: (title, message = '') =>
        enqueue({ type: 'alert', title, message }).then(() => {}),
      confirm: (title, message, opts) =>
        enqueue({
          type: 'confirm',
          title,
          message,
          confirmLabel: opts?.confirmLabel,
          confirmStyle: opts?.confirmStyle,
        }) as Promise<boolean>,
      prompt: (title, message = '', defaultValue = '') =>
        enqueue({ type: 'prompt', title, message, defaultValue }) as Promise<string | null>,
    }),
    [enqueue],
  );

  // Value returned when the user dismisses without pressing OK.
  const dismissValue = dialog
    ? dialog.type === 'alert' ? true : dialog.type === 'confirm' ? false : null
    : null;

  return (
    <AppDialogContext.Provider value={api}>
      {children}

      {dialog && (
        <Modal transparent visible animationType="fade" onRequestClose={() => dismiss(dismissValue)}>
          <Pressable style={s.overlay} onPress={() => dismiss(dismissValue)}>
            <Pressable style={s.card} onPress={(e) => e.stopPropagation()}>
              {/* Title */}
              <Text style={s.title}>{dialog.title}</Text>

              {/* Message */}
              {dialog.message ? <Text style={s.message}>{dialog.message}</Text> : null}

              {/* Prompt input */}
              {dialog.type === 'prompt' && (
                <TextInput
                  style={s.input}
                  value={promptValue}
                  onChangeText={setPromptValue}
                  autoFocus
                  placeholder="Type here…"
                  placeholderTextColor="#9CA3AF"
                />
              )}

              {/* Buttons */}
              <View style={s.buttons}>
                {/* Cancel (confirm / prompt only) */}
                {dialog.type !== 'alert' && (
                  <TouchableOpacity
                    style={[s.btn, s.btnCancel]}
                    onPress={() => dismiss(dismissValue)}
                  >
                    <Text style={s.btnCancelText}>Cancel</Text>
                  </TouchableOpacity>
                )}

                {/* OK / Confirm */}
                <TouchableOpacity
                  style={[
                    s.btn,
                    s.btnOk,
                    dialog.confirmStyle === 'destructive' && s.btnDestructive,
                  ]}
                  onPress={() =>
                    dismiss(dialog.type === 'prompt' ? promptValue : true)
                  }
                >
                  <Text
                    style={[
                      s.btnOkText,
                      dialog.confirmStyle === 'destructive' && s.btnDestructiveText,
                    ]}
                  >
                    {dialog.confirmLabel ?? 'OK'}
                  </Text>
                </TouchableOpacity>
              </View>
            </Pressable>
          </Pressable>
        </Modal>
      )}
    </AppDialogContext.Provider>
  );
}

// ─── Hook ───────────────────────────────────────────────────────────────────

export function useAppDialog(): AppDialogAPI {
  return useContext(AppDialogContext);
}

// ─── Styles ─────────────────────────────────────────────────────────────────

const s = StyleSheet.create({
  overlay: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.45)',
    justifyContent: 'center',
    alignItems: 'center',
    padding: 24,
  },
  card: {
    backgroundColor: '#FFFFFF',
    borderRadius: 16,
    padding: 24,
    width: '100%',
    maxWidth: 360,
  },
  title: {
    fontSize: 18,
    fontWeight: '700',
    color: '#111827',
    marginBottom: 8,
  },
  message: {
    fontSize: 15,
    color: '#4B5563',
    lineHeight: 22,
    marginBottom: 20,
  },
  input: {
    borderWidth: 1,
    borderColor: '#D1D5DB',
    borderRadius: 10,
    paddingHorizontal: 14,
    paddingVertical: 10,
    fontSize: 15,
    color: '#111827',
    marginBottom: 20,
  },
  buttons: {
    flexDirection: 'row',
    gap: 10,
  },
  btn: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 10,
    alignItems: 'center',
  },
  btnCancel: {
    backgroundColor: '#F3F4F6',
  },
  btnCancelText: {
    fontSize: 15,
    fontWeight: '600',
    color: '#4B5563',
  },
  btnOk: {
    backgroundColor: '#111827',
  },
  btnOkText: {
    fontSize: 15,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  btnDestructive: {
    backgroundColor: '#EF4444',
  },
  btnDestructiveText: {
    color: '#FFFFFF',
  },
});
