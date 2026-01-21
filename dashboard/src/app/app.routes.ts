import { Routes } from '@angular/router';
import { DashboardPage } from '../pages/dashboard-page/dashboard-page';

export const routes: Routes = [
  { title: 'Dashboard', path: 'dashboard', component: DashboardPage },
  { title: 'Dashboard', path: '', component: DashboardPage },
];
