# Project development

Read TECHNICAL_DESIGN.md before changing the runtime. Keep the design and validation record current with code changes. Preserve attempt authentication, one execution slot, separate delivery, and reconciliation before replaying uncertain writes. Use isolated test state; do not start a second receiver or copy credentials into the repository.
