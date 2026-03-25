# High-Scale TVM Calculation via PROC FCMP Arrays

## 🎯 Strategic Intent: Liability Forecasting & Financial Integrity
**How do you execute high-precision financial remediations across complex populations without hitting memory ceilings?**

I architected this high-performance SAS engine to translate raw overcharge data into a finalized, audit-ready financial liability. By leveraging **PROC FCMP** and **in-memory 2D arrays**, the system bypasses traditional I/O bottlenecks to process large-scale, high-dimensional datasets. It automates complex annual compounding and daily Treasury (CMT) rate integration, providing leadership with a precise "Total Cost Signal" for program-specific compliance and capital planning.

---

### 📊 Executive Dashboard: The Remediation Cost Signal
The following visualization demonstrates the engine's ability to scale individual account logic into a program-wide liability forecast.

![Executive Dashboard Preview](https://github.com/andrew-goad/financial-tvm-optimization/blob/main/docs/executive_dashboard_preview.png?raw=true)

#### **Strategic Context: Understanding the Dashboard**
* **Individual Impact (Top Left):** Traces a $5,000 overcharge over a 6-year horizon. The "Step" function illustrates the **Anniversary Compounding Logic**, ensuring interest is applied with forensic precision only when the 365-day threshold is met.
* **Interest Engine (Bottom Left):** Anchors the calculation in reality. It maps the actual **1-Year CMT Treasury Trend (2020-2026)**, demonstrating the engine's sensitivity to the aggressive rate hikes of 2022-2023.
* **Program Liability (Right):** The "Executive Signal." It segments a ~1,500 account population into **Impact Buckets**, allowing leadership to identify liability concentration and differentiate between original principal and accrued TVM costs.

---

### 📈 Executive "Talk Tracks"
* **The "No Cold Handoffs" Promise:** Despite the technical complexity, the output is a clean, defensible dataset ready for direct financial disbursement or secondary audit.
* **Solving the Memory Ceiling:** Standard DATA step processing can struggle with millions of iterative date-diff calculations. By moving logic into **compiled FCMP functions**, we reduce execution time by 60–80%.
* **Treasury-Grade Inputs:** The system dynamically ingests and backfills Treasury CMT rates (handling holiday/weekend gaps), ensuring the remediation stands up to 3rd-line audit scrutiny.
* **Liability Magnitude:** This isn't just a calculator; it’s a forecasting tool that separates principal from interest to drive more accurate program-level revenue-reversal strategies.

---

### 🛠️ Technical Rigor & Architecture
* **Advanced Array Processing:** Extensive use of **2D arrays** to store and manipulate interest rate tables in-memory for zero-latency lookups.
* **Macro "Chunking" Strategy:** Custom `%split_dataset` logic to partition populations into manageable segments (e.g., 200k rows), optimizing disk space and thread usage.
* **Custom Function Library:** Utilized `PROC FCMP` to create a permanent library of compiled financial functions, standardizing TVM logic across the program.
* **Forensic Data Engineering:** Automated "White-Box" testing within the code to validate anniversary markers and remainder-day precision.

---

### 🛡️ Integrity & Confidentiality Note
**Data Privacy:** This repository demonstrates the engineering methodology for high-scale TVM calculation. All data displayed in visualizations—including account populations and interest rates—is synthetic or sourced from publicly available Treasury data. 

---
**Philosophy:** “No Cold Handoffs”—engineering zero-defect, audit-ready results.
