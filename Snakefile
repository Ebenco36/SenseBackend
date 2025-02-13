# Snakefile

rule all:
    input:
        "Data/output/ovid_papers_tags",
        "Data/output/cochrane_papers_tags",
        "Data/output/love_papers_tags",
        "Data/output/medline_papers_tags"

rule process_ovid:
    output:
        "Data/output/ovid_papers_tags"
    log:
        "logs/process_ovid.log"
    shell:
        """
        python process_papers.py "Ovid" "SELECT upper('DOI') FROM ovid_db WHERE primary_id IN (1, 2, 3, 4, 5)" "{output}" &> {log}
        """

rule process_cochrane:
    output:
        "Data/output/cochrane_papers_tags"
    log:
        "logs/process_cochrane.log"
    shell:
        """
        python process_papers.py "Cochrane" "SELECT upper('doi'), doi_link FROM cochrane_db WHERE primary_id IN (1, 2)" "{output}" &> {log}
        """

rule process_love:
    output:
        "Data/output/love_papers_tags"
    log:
        "logs/process_love.log"
    shell:
        """
        python process_papers.py "Love" "SELECT upper('doi') FROM love_db WHERE primary_id IN (1, 2)" "{output}" &> {log}
        """

rule process_medline:
    output:
        "Data/output/medline_papers_tags"
    log:
        "logs/process_medline.log"
    shell:
        """
        python process_papers.py "Medline" "SELECT upper('DOI') FROM medline_db WHERE primary_id IN (1, 2)" "{output}" &> {log}
        """
