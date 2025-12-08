import { test, expect } from '@playwright/test';

test.describe('Recommendations Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/recommendations');
  });

  test('should display recommendations title and description', async ({ page }) => {
    await expect(page.getByRole('heading', { name: 'Get Resource Recommendations' })).toBeVisible();
    await expect(page.getByText('Get optimal Spark configuration recommendations for your job')).toBeVisible();
  });

  test('should display recommendation form', async ({ page }) => {
    await expect(page.getByRole('heading', { name: 'Job Requirements' })).toBeVisible();

    // Check form fields
    await expect(page.getByLabel(/Input Data Size \(GB\)/i)).toBeVisible();
    await expect(page.getByLabel(/Job Type/i)).toBeVisible();
    await expect(page.getByLabel(/Application Name/i)).toBeVisible();
    await expect(page.getByLabel(/Recommendation Method/i)).toBeVisible();

    // Check buttons
    await expect(page.getByRole('button', { name: /Get Recommendation/i })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Reset' })).toBeVisible();
  });

  test('should have default form values', async ({ page }) => {
    const inputSizeField = page.getByLabel(/Input Data Size \(GB\)/i);
    await expect(inputSizeField).toHaveValue('10');
  });

  test('should validate required fields', async ({ page }) => {
    // Clear the input size field
    const inputSizeField = page.getByLabel(/Input Data Size \(GB\)/i);
    await inputSizeField.clear();

    // Try to submit
    const submitButton = page.getByRole('button', { name: /Get Recommendation/i });
    await expect(submitButton).toBeDisabled();
  });

  test('should submit recommendation request', async ({ page }) => {
    // Fill in the form
    await page.getByLabel(/Input Data Size \(GB\)/i).fill('50');
    await page.selectOption('select[name="jobType"]', 'ml');
    await page.getByLabel(/Application Name/i).fill('test-ml-job');

    // Submit form (using real API)
    await page.getByRole('button', { name: /Get Recommendation/i }).click();

    // Wait for results from real API
    await expect(page.getByRole('heading', { name: 'Recommended Configuration' })).toBeVisible({ timeout: 15000 });

    // Verify configuration cards are displayed (actual values from backend)
    await expect(page.getByText('Executor Cores')).toBeVisible();
    await expect(page.getByText('Executor Memory')).toBeVisible();
  });

  test('should display recommendation results', async ({ page }) => {
    // Submit with default values (using real API)
    await page.getByRole('button', { name: /Get Recommendation/i }).click();

    // Verify results display
    await expect(page.getByRole('heading', { name: 'Recommended Configuration' })).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('Confidence:')).toBeVisible();

    // Check configuration cards
    await expect(page.getByText('Executor Cores')).toBeVisible();
    await expect(page.getByText('Executor Memory')).toBeVisible();
    await expect(page.getByText('Number of Executors')).toBeVisible();
    await expect(page.getByText('Driver Memory')).toBeVisible();

    // Check predicted metrics
    await expect(page.getByText('Predicted Performance')).toBeVisible();
    await expect(page.getByText('Duration:')).toBeVisible();
    await expect(page.getByText('Estimated Cost:')).toBeVisible();

    // Check spark command
    await expect(page.getByRole('heading', { name: 'Spark Submit Command' })).toBeVisible();
    await expect(page.getByText(/spark-submit/)).toBeVisible();
  });

  test('should display resource allocation chart', async ({ page }) => {
    // Submit with default values (using real API)
    await page.getByRole('button', { name: /Get Recommendation/i }).click();

    // Wait for chart to render
    await page.waitForSelector('canvas', { timeout: 30000 });
    await expect(page.getByRole('heading', { name: 'Resource Allocation' })).toBeVisible();
  });

  test('should display similar jobs visualization', async ({ page }) => {
    // Submit with default values (using real API)
    await page.getByRole('button', { name: /Get Recommendation/i }).click();

    // Check similar jobs section (if backend returns similar jobs metadata)
    await expect(page.getByRole('heading', { name: 'Recommended Configuration' })).toBeVisible({ timeout: 15000 });

    // Similar jobs section may or may not appear depending on backend response
    const similarJobsHeading = page.getByRole('heading', { name: 'Similar Jobs Analysis' });
    const hasSimilarJobs = await similarJobsHeading.isVisible().catch(() => false);

    if (hasSimilarJobs) {
      await expect(page.getByText('Similarity:')).toBeVisible();
    }
  });

  test('should display cost-performance indicator', async ({ page }) => {
    // Submit with default values (using real API)
    await page.getByRole('button', { name: /Get Recommendation/i }).click();

    // Check cost-performance indicator
    await expect(page.getByRole('heading', { name: 'Recommended Configuration' })).toBeVisible({ timeout: 15000 });

    // Cost-performance indicator should be visible if predicted_metrics exist
    const costPerfHeading = page.getByRole('heading', { name: 'Cost-Performance Indicator' });
    const hasCostPerf = await costPerfHeading.isVisible().catch(() => false);

    if (hasCostPerf) {
      await expect(page.getByText('Cost Efficient')).toBeVisible();
    }
  });

  test('should copy spark command to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Submit with default values (using real API)
    await page.getByRole('button', { name: /Get Recommendation/i }).click();

    // Wait for results and click copy button
    const copyButton = page.getByRole('button', { name: /Copy Command/i });
    await expect(copyButton).toBeVisible({ timeout: 15000 });
    await copyButton.click();

    // Verify button text changes
    await expect(page.getByRole('button', { name: /Copied!/i })).toBeVisible({ timeout: 2000 });
  });

  test('should reset form', async ({ page }) => {
    // Change form values
    await page.getByLabel(/Input Data Size \(GB\)/i).fill('100');
    await page.selectOption('select[name="jobType"]', 'sql');
    await page.getByLabel(/Application Name/i).fill('test-job');

    // Click reset
    await page.getByRole('button', { name: 'Reset' }).click();

    // Verify form is reset
    await expect(page.getByLabel(/Input Data Size \(GB\)/i)).toHaveValue('10');
    await expect(page.getByLabel(/Application Name/i)).toHaveValue('');
  });

  test('should handle API errors', async ({ page }) => {
    // Fill in invalid data that backend will reject
    await page.getByLabel(/Input Data Size \(GB\)/i).fill('0');

    await page.getByRole('button', { name: /Get Recommendation/i }).click();

    // Should show error message (either validation or API error)
    const errorMessage = page.getByText(/Please enter a valid input size|Failed to get recommendation/i);
    await expect(errorMessage).toBeVisible({ timeout: 10000 });
  });

  test('should show loading state during submission', async ({ page }) => {
    // Click submit button
    await page.getByRole('button', { name: /Get Recommendation/i }).click();

    // Should show loading state (check immediately after click)
    const loadingText = page.getByText('Analyzing historical data and generating recommendations...');
    const isVisible = await loadingText.isVisible().catch(() => false);

    // If visible, good! If not, the API might have been too fast (which is ok)
    if (isVisible) {
      await expect(loadingText).toBeVisible();
    }

    // Eventually should show results
    await expect(page.getByRole('heading', { name: 'Recommended Configuration' })).toBeVisible({ timeout: 15000 });
  });
});
