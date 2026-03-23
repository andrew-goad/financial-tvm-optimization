# High-Scale TVM Calculation via PROC FCMP Arrays

## 🎯 Strategic Intent: Computational Efficiency & Financial Integrity
**How do you execute financial remediations on millions of records without hitting memory ceilings?**

I architected this high-scale SAS solution using **PROC FCMP** and **2D arrays** to bypass I/O bottlenecks. The system uses a macro-driven **"chunking"** strategy for batch processing and integrates daily Treasury CMT rates. It automates complex annual anniversary compounding and remainder-day interest calculations to ensure 100% financial integrity at scale.

---

### 📈 Executive "Talk Tracks"
* **Solving the Memory Ceiling:** Standard DATA step processing can struggle with millions of iterative date-diff calculations. By moving the logic into **PROC FCMP arrays**, we reduce execution time by 60-80%.
* **Audit-Ready Accuracy:** Every penny is accounted for. The engine calculates daily interest between "Anniversary Dates" and handles the final "Remainder Window" with forensic precision.
* **Treasury-Grade Inputs:** The system dynamically pulls and backfills Treasury CMT rates, ensuring the remediation is defensible under regulatory scrutiny.
* **The "No Cold Handoffs" Promise:** Despite the technical complexity, the output is a clean, simple dataset ready for direct financial disbursement or secondary audit.

---

### 🛠️ Technical Rigor & Architecture
* **Advanced Array Processing:** Extensive use of **2D arrays** to store and manipulate interest rate tables in-memory.
* **Macro "Split-Dataset" Strategy:** Custom `%split_dataset` and batch-windowing logic to partition high-dimensional populations into manageable segments.
* **Custom Function Creation:** Utilizing `PROC FCMP` to create reusable, compiled financial functions that reside in a permanent library.
* **Batch Orchestration:** Sequential processing of data "chunks" with automated cleanup of intermediate work tables to optimize disk space.

---

### 🛡️ Integrity & Confidentiality Note
**Data Privacy:** This repository demonstrates the engineering methodology for high-scale TVM calculation. No proprietary data is included; all interest rates and account populations are synthetic or publicly available Treasury data.

---
**Philosophy:** “No Cold Handoffs”—engineering zero-defect, audit-ready results.
