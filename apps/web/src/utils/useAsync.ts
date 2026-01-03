import type { DependencyList } from "react";
import { useCallback, useEffect, useReducer, useRef, useState } from "react";

export interface AsyncData<T> {
  data: T;
  loading: boolean;
  error: string | null;
  reload: () => void;
  cancel: () => void;
  reset: () => void;
  setData: (value: T | ((prev: T) => T)) => void;
}

export interface UseAsyncDataOptions {
  keepDataOnError?: boolean;
  onError?: (message: string) => void;
}

export function useAsyncData<T>(
  enabled: boolean,
  request: () => Promise<T>,
  deps: DependencyList,
  initialData: T,
  options: UseAsyncDataOptions = {}
): AsyncData<T> {
  const [data, setData] = useState<T>(initialData);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reloadToken, bumpReloadToken] = useReducer((x: number) => x + 1, 0);

  const seqRef = useRef(0);
  const initialDataRef = useRef(initialData);
  initialDataRef.current = initialData;

  const keepDataOnErrorRef = useRef(false);
  keepDataOnErrorRef.current = options.keepDataOnError === true;

  const onErrorRef = useRef<UseAsyncDataOptions["onError"]>(undefined);
  onErrorRef.current = options.onError;

  const cancel = useCallback(() => {
    seqRef.current++;
    setLoading(false);
  }, []);

  const reload = useCallback(() => {
    if (!enabled) return;
    seqRef.current++;
    setLoading(true);
    setError(null);
    bumpReloadToken();
  }, [enabled, bumpReloadToken]);

  const reset = useCallback(() => {
    seqRef.current++;
    setData(initialDataRef.current);
    setLoading(false);
    setError(null);
  }, []);

  useEffect(() => {
    if (!enabled) return;
    const seq = ++seqRef.current;
    setLoading(true);
    setError(null);
    request()
      .then((r) => {
        if (seq !== seqRef.current) return;
        setData(r);
      })
      .catch((e) => {
        if (seq !== seqRef.current) return;
        const message = e instanceof Error ? e.message : String(e);
        setError(message);
        const onError = onErrorRef.current;
        if (onError) onError(message);
        if (!keepDataOnErrorRef.current) setData(initialDataRef.current);
      })
      .finally(() => {
        if (seq !== seqRef.current) return;
        setLoading(false);
      });
  }, [enabled, reloadToken, ...deps]);

  return { data, loading, error, reload, cancel, reset, setData };
}
