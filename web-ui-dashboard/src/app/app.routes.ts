import { Routes } from '@angular/router';
import { Dashboard } from './components/dashboard/dashboard';
import { Recommendations } from './components/recommendations/recommendations';
import { ChartsComponent } from './components/charts/charts';

export const routes: Routes = [
  { path: '', redirectTo: '/dashboard', pathMatch: 'full' },
  { path: 'dashboard', component: Dashboard },
  { path: 'analytics', component: ChartsComponent },
  { path: 'recommendations', component: Recommendations },
  { path: '**', redirectTo: '/dashboard' }
];
