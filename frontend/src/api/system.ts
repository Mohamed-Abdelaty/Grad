import { apiClient, setApiToken } from './client';

export interface ServiceStatus {
  connected: boolean;
  error: string | null;
  latency_ms?: number;
  // Kafka-specific
  broker_count?: number;
  topic_count?: number;
  // Spark-specific
  worker_count?: number;
  status?: string;
  // HDFS-specific
  namenode_url?: string;
  num_live_datanodes?: number;
}

export interface SystemStatus {
  status: 'ok' | 'degraded';
  database: ServiceStatus;
  kafka: ServiceStatus;
  spark: ServiceStatus;
  hdfs: ServiceStatus;
}

export async function getSystemStatus(): Promise<SystemStatus> {
  const res = await apiClient.get<SystemStatus>('/api/system/status');
  return res.data;
}

export async function login(
  username: string,
  password: string
): Promise<{ access_token: string; token_type: string }> {
  const formData = new URLSearchParams();
  formData.append('username', username);
  formData.append('password', password);

  const res = await apiClient.post<{ access_token: string; token_type: string }>(
    '/api/auth/login',
    formData,
    { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
  );

  setApiToken(res.data.access_token);
  return res.data;
}

export async function getMe(): Promise<{ username: string; role: string }> {
  const res = await apiClient.get<{ username: string; role: string }>('/api/auth/me');
  return res.data;
}
