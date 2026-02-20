const API_BASE = '/api/v1';
const TENANT_ID = '550e8400-e29b-41d4-a716-446655440000'; // Demo tenant

// API Service with polling support
class ApiService {
  constructor() {
    this.pollingIntervals = new Map();
  }

  getHeaders(additionalHeaders = {}) {
    return {
      'Content-Type': 'application/json',
      'X-Tenant-ID': TENANT_ID,
      ...additionalHeaders,
    };
  }

  async get(endpoint) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      headers: this.getHeaders(),
    });
    if (!response.ok) {
      throw new Error(`API Error: ${response.statusText}`);
    }
    return response.json();
  }

  async post(endpoint, data) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify(data),
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'API Error');
    }
    return response.json();
  }

  async put(endpoint, data) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      method: 'PUT',
      headers: this.getHeaders(),
      body: JSON.stringify(data),
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'API Error');
    }
    return response.json();
  }

  async delete(endpoint) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      method: 'DELETE',
      headers: this.getHeaders(),
    });
    if (!response.ok) {
      throw new Error(`API Error: ${response.statusText}`);
    }
    return response.json();
  }

  // Polling helper
  startPolling(key, callback, interval = 2000) {
    if (this.pollingIntervals.has(key)) {
      return; // Already polling
    }

    const id = setInterval(callback, interval);
    this.pollingIntervals.set(key, id);
    callback(); // Execute immediately
  }

  stopPolling(key) {
    const id = this.pollingIntervals.get(key);
    if (id) {
      clearInterval(id);
      this.pollingIntervals.delete(key);
    }
  }

  stopAllPolling() {
    this.pollingIntervals.forEach(id => clearInterval(id));
    this.pollingIntervals.clear();
  }
}

const api = new ApiService();

// Cow API
export const cowApi = {
  // Get all cows from SQL projection
  async getCows() {
    const response = await api.get('/cows');
    // Backend returns paginated response: {items: [], total: 0, ...}
    // Ensure we always return an array
    if (Array.isArray(response)) {
      // Handle case where backend returns array directly (shouldn't happen but be defensive)
      return response;
    }
    if (response && Array.isArray(response.items)) {
      return response.items;
    }
    // Fallback to empty array
    console.warn('Unexpected response format from /cows:', response);
    return [];
  },

  // Get single cow by ID
  async getCow(cowId) {
    return api.get(`/cows/${cowId}`);
  },

  // Create new cow
  async createCow(data) {
    return api.post('/cows', {
      tag_number: data.tag_number,
      name: data.name || undefined,
      breed: data.breed,
      birth_date: data.birth_date,
      sex: data.sex,
    });
  },

  // Update cow breed
  async updateCowBreed(cowId, breed) {
    return api.put(`/cows/${cowId}/breed`, { breed });
  },

  // Record new weight measurement
  async recordWeight(cowId, weightData) {
    return api.post(`/cows/${cowId}/weight`, {
      weight_kg: weightData.weight_kg,
      measurement_date: weightData.measurement_date || new Date().toISOString(),
      measured_by: weightData.measured_by || 'webapp_user',
      measurement_method: weightData.measurement_method || 'manual',
      notes: weightData.notes || null,
    });
  },

  // Deactivate cow (mark as sold)
  async deactivateCow(cowId, reason = 'sold') {
    const response = await fetch(`${API_BASE}/cows/${cowId}`, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
        'X-Tenant-ID': TENANT_ID,
      },
      body: JSON.stringify({
        reason: reason,
        deactivation_date: null, // defaults to today on backend
        notes: null
      }),
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || error.message || 'Failed to deactivate cow');
    }
    return response.json();
  },

  // Get cow lifecycle analytics (queries event history directly)
  async getCowAnalytics(cowId) {
    try {
      const response = await api.get(`/analytics/cow/${cowId}?tenant_id=${TENANT_ID}`);
      // Transform response to match frontend expectations
      return {
        cow_id: response.data.cow_id,
        weight_trend: response.data.measurements.map(m => ({
          date: m.date,
          weight: m.weight_kg
        })),
        total_events: response.data.measurements.length,
        average_weight: response.data.average_weight_kg,
        weight_change: response.data.weight_change_kg
      };
    } catch (error) {
      console.warn('Analytics not available:', error.message);
      // Return mock data if analytics not available
      return {
        cow_id: cowId,
        weight_trend: [],
        total_events: 0,
      };
    }
  },

  // Get herd composition analytics (DuckDB queries Gold Delta directly)
  async getHerdComposition(date = null) {
    try {
      // Default to today's UTC date if not specified
      if (!date) {
        const now = new Date();
        const utcDate = new Date(now.getTime() + now.getTimezoneOffset() * 60000);
        date = utcDate.toISOString().split('T')[0]; // Format: YYYY-MM-DD
      }
      const endpoint = `/analytics/herd-composition?tenant_id=${TENANT_ID}&date=${date}`;
      const response = await api.get(endpoint);
      return response;
    } catch (error) {
      console.warn('Herd composition not available:', error.message);
      return null;
    }
  },
};

// Sync API
export const syncApi = {
  // Get sync status
  async getSyncStatus() {
    try {
      return await api.get('/sync/status');
    } catch (error) {
      return {
        table_name: 'cows',
        sync_lag_seconds: null,
        last_sync_completed_at: null,
        total_rows_synced: 0,
        status: 'unknown',
      };
    }
  },

  // Get unpublished events count
  async getUnpublishedEvents() {
    try {
      const response = await api.get('/events/unpublished');
      return response.count || 0;
    } catch (error) {
      return 0;
    }
  },
};

// Events API
export const eventsApi = {
  // Get events for a cow
  async getCowEvents(cowId) {
    try {
      return await api.get(`/events/cow/${cowId}`);
    } catch (error) {
      return [];
    }
  },

  // Get recent events
  async getRecentEvents(limit = 10) {
    try {
      return await api.get(`/events/recent?limit=${limit}`);
    } catch (error) {
      return [];
    }
  },
};

// Polling helpers
export const polling = {
  startCowsPolling(callback) {
    api.startPolling('cows', callback, 1000);
  },

  startSyncPolling(callback) {
    api.startPolling('sync', callback, 1000);
  },

  startCowPolling(cowId, callback) {
    api.startPolling(`cow-${cowId}`, callback, 1000);
  },

  stopCowsPolling() {
    api.stopPolling('cows');
  },

  stopSyncPolling() {
    api.stopPolling('sync');
  },

  stopCowPolling(cowId) {
    api.stopPolling(`cow-${cowId}`);
  },

  stopAll() {
    api.stopAllPolling();
  },
};

export default api;
