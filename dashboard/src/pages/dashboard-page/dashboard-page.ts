import { ChangeDetectorRef, Component, inject, ViewChild } from '@angular/core';
import { DashboardService } from '../../services/dashboard.service';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';
import { CommonModule } from '@angular/common';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatNativeDateModule } from '@angular/material/core';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { FormBuilder, FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';

@Component({
  selector: 'app-dashboard-page',
  imports: [CommonModule, MatTableModule, MatPaginatorModule, MatDatepickerModule, MatNativeDateModule, MatFormFieldModule, MatDatepickerModule, MatSelectModule, FormsModule, MatInputModule, ReactiveFormsModule],
  providers: [
    MatDatepickerModule,
  ],
  templateUrl: './dashboard-page.html',
  styleUrl: './dashboard-page.css',
})
export class DashboardPage {
  fb = inject(FormBuilder);
  service = inject(DashboardService);
  jobsQuantity: JobsReponse | null = null;
  enrichmentResponse: DataEnrinchmentResponse[] | null = null;
  changeRef = inject(ChangeDetectorRef);

  statuses = [
    {
      value: 'CONCLUIDO',
      viewValue: 'Finished',
    },
    {
      value: 'FALHOU',
      viewValue: 'Failed',
    },
    {
      value: 'CANCELADO',
      viewValue: 'Canceled',
    },
    {
      value: 'EM_PROCESSAMENTO',
      viewValue: 'Processing',
    },
  ];

  form!: FormGroup;

  enrichmentTableSource: MatTableDataSource<DataEnrinchmentResponse> | null = null;

  topEnrichmentTableSource: MatTableDataSource<DataEnrinchmentResponse> | null = null;

  topEnrichmentsDisplayedColumns: string[] = [
    'nome_workspace',
    'total_contatos',
  ];

  enrichmentTableDisplayedColumns: string[] = [
    'nome_workspace',
    'total_contatos',
    'tipo_contato',
    'status_processamento',
    'duracao_processamento_minutos',
    'tempo_por_contato_minutos',
    'processamento_sucesso',
    'categoria_tamanho_job',
    'necessita_reprocessamento',
    'data_criacao',
    'data_atualizacao',
  ];

  @ViewChild(MatPaginator) paginator!: MatPaginator;

  ngOnInit() {

    this.form = this.fb.group({
      id: [''],
      status: [''],
      dateCreated: this.fb.group({
        start: [null],
        end: [null],
      }),
    });

    this.form.valueChanges.subscribe(x => this.applyFilters());

    this.service.getOverview().subscribe(response => { this.jobsQuantity = response as JobsReponse; this.changeRef.detectChanges(); });
    this.service.getEnrichments().subscribe(response => {
      this.enrichmentResponse = (response as any).data as DataEnrinchmentResponse[] ?? [];
      this.enrichmentTableSource = new MatTableDataSource<DataEnrinchmentResponse>(this.enrichmentResponse);
      this.enrichmentTableSource!.paginator = this.paginator;
    });
    this.service.getWorkspaceTop().subscribe(response => {
      const data: DataEnrinchmentResponse[] = response;
      this.topEnrichmentTableSource = new MatTableDataSource<DataEnrinchmentResponse>(data);
      console.log(response);
      this.changeRef.detectChanges();
    });
  }

  applyFilters() {
    const { id, status, dateCreated } = this.form.value;
    this.service.getEnrichments(id, status, dateCreated.end, dateCreated.start).subscribe(response => {
      this.enrichmentResponse = (response as any).data as DataEnrinchmentResponse[] ?? [];
      this.enrichmentTableSource = new MatTableDataSource<DataEnrinchmentResponse>(this.enrichmentResponse);
      this.enrichmentTableSource!.paginator = this.paginator;
    });
  }

}

interface JobsReponse {
  total_jobs: number;
  small_jobs: number;
  medium_jobs: number;
  large_jobs: number;
  very_large_jobs: number;
}

interface DataEnrinchmentResponse {
  id_enriquecimento: string;
  id_workspace: string;
  nome_workspace: string;
  total_contatos: number;
  tipo_contato: string;
  status_processamento: string;
  duracao_processamento_minutos: number;
  tempo_por_contato_minutos: number;
  processamento_sucesso: boolean;
  categoria_tamanho_job: string;
  necessita_reprocessamento: boolean;
  data_criacao: Date;
  data_atualizacao: Date;
  data_atualizacao_dw: Date;
}
