/// <reference types="vite/client" />
declare global { interface Window { koreNext?: { openExternal(url: string): Promise<void>; notify(title: string, body: string): Promise<void>; pickDirectory(): Promise<string | null> } } }
export {}

