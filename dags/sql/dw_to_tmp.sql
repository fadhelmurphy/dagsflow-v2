-- logical date: {{ .ds }}  run: {{ .run_id }}
SELECT * FROM `{{ .project_id }}.{{ .target_dataset }}.{{ .target_table }}` LIMIT 10
