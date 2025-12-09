import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';

export interface AppConfig {
  apiUrl: string;
}

@Injectable({
  providedIn: 'root'
})
export class ConfigService {
  private config: AppConfig | null = null;

  constructor(private http: HttpClient) {}

  async loadConfig(): Promise<void> {
    try {
      this.config = await firstValueFrom(
        this.http.get<AppConfig>('/config.json')
      );
    } catch (error) {
      console.error('Failed to load config.json, using defaults:', error);
      this.config = {
        apiUrl: 'http://localhost:8080'
      };
    }
  }

  get apiUrl(): string {
    return this.config?.apiUrl ?? 'http://localhost:8080';
  }

  getConfig(): AppConfig {
    if (!this.config) {
      throw new Error('Configuration not loaded. Ensure ConfigService.loadConfig() is called during app initialization.');
    }
    return this.config;
  }
}
