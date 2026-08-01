# 05 Dynamic Logic & Patterns

Reusable machinery: parameter-driven switching, dispatch, formatting and windowing. These patterns repeat across the model, so they are worth reading once carefully.

Read in file order — each measure builds on the ones before it.

- **`01_KPI_Title_Selected.dax`** — Header text following the selection.
- **`02_KPI_Display_Selected.dax`** — The value behind the selected KPI.
- **`03_Matrix_Metric_1.dax`** — First column of a field-parameter matrix.
- **`04_Geo_Level_Selected.dax`** — Resolves the active geography grain.
- **`05_Channel_Metric_Mode.dax`** — Switches between channel and vendor views.
- **`06_DaysInFilter.dax`** — Days in the current selection.
- **`07_MaxDateInFilter.dax`** — Latest date in context.
- **`08_Overall_Fit_Score.dax`** — Composite attribution model fit.
- **`09_Behavioral_Fit.dax`** — Does the model match observed journeys?
- **`10_Operational_Trust.dax`** — Would the recommendation survive a model change?
- **`11_Recommended_Model.dax`** — Names the best-fitting model.
- **`12_Vendor_Action_Quadrant.dax`** — INVEST / OPTIMIZE / CUT / SCALE TEST.
- **`13_Bias_Closing.dax`** — How far a model favours the closing touch.
- **`14_GPI_Score_CF_Color.dax`** — Conditional formatting colour.
