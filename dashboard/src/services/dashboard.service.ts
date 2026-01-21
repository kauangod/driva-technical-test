import { HttpClient, HttpParams } from "@angular/common/http";
import { inject, Injectable, Query } from "@angular/core";
import { environment } from "../environments/environment";
import { map } from "rxjs";




@Injectable({
  providedIn: 'root'
})
export class DashboardService {
  httpClient = inject(HttpClient);
  baseUrl = environment.apiUrl + '/analytics';

  getOverview() {
    return this.httpClient.get(this.baseUrl + '/overview');
  }

  getEnrichments(id: string = '', status: string = '', endDate: Date | undefined = undefined, startDate: Date | undefined = undefined) {
    let params = new HttpParams();
    if (id.length > 0) params = params.append('workspace_id', id);
    if (status.length > 0) params = params.append('processing_status', status);
    if (!!startDate) params = params.append('start_date', startDate.toISOString());
    if (!!endDate) params = params.append('end_date', endDate.toISOString());
    return this.httpClient.get(this.baseUrl + '/enrichments/status', { params });
  }

  getWorkspaceTop() {
    return this.httpClient.get(this.baseUrl + '/workspaces/top').pipe(map(response => (response as any).data.length > 5 ? (response as any).data.slice(0, 5) : (response as any).data));
  }
}
