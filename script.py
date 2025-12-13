import argparse
import pandas as pd
from pathlib import Path

DOMINANT_MIN_SHARE = 0.6  

# função a ser retirada
def load_prospect_companies(path: str | None) -> set[str]:
    if not path:
        return set()

    p = Path(path)
    if not p.exists():
        print(f"[AVISO] Arquivo de empresas prospectadas não encontrado: {path}")
        return set()

    if p.suffix.lower() == ".txt":
        companies = {line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()}
    else:
        df = pd.read_csv(p)
        col_candidates = [c for c in df.columns if c.lower().strip() in {"company name", "company", "empresa"}]
        if not col_candidates:
            raise ValueError("Não encontrei coluna de nomes de empresa em empresas_prospectadas.")
        col = col_candidates[0]
        companies = set(df[col].dropna().astype(str).str.strip())

    return companies

def extract_domain(email: str) -> str | None:
    if not isinstance(email, str) or "@" not in email:
        return None
    return email.split("@")[-1].strip().lower()

def find_dominant_company_per_domain(df: pd.DataFrame) -> pd.DataFrame:
    stats = (
        df.groupby(["domain", "Company Name"])
        .size()
        .reset_index(name="count")
    )

    total_per_domain = stats.groupby("domain")["count"].sum().rename("total_domain")
    stats = stats.merge(total_per_domain, on="domain")
    stats["share"] = stats["count"] / stats["total_domain"]

    dominant = (
        stats.sort_values(["domain", "count"], ascending=[True, False])
        .drop_duplicates("domain")
        .rename(columns={
            "Company Name": "dominant_company",
            "share": "dominant_share"
        })[["domain", "dominant_company", "dominant_share"]]
    )

    return dominant

def find_canonical_company_by_name(df: pd.DataFrame) -> pd.DataFrame:
    # encontra, para cada variação de nome de empresa, qual é a forma mais frequente.
    temp = df["Company Name"].astype(str).str.strip()
    name_stats = (
        pd.DataFrame({"Company Name": temp})
        .assign(company_norm=lambda s: s["Company Name"].str.lower())
        .groupby(["company_norm", "Company Name"])
        .size()
        .reset_index(name="count")
    )

    canonical = (
        name_stats.sort_values(["company_norm", "count"], ascending=[True, False])
        .drop_duplicates("company_norm")
        .rename(columns={"Company Name": "canonical_company"})[
            ["company_norm", "canonical_company"]
        ]
    )

    return canonical

def mark_suspects(df: pd.DataFrame, prospect_companies: set[str]) -> pd.DataFrame:
    """
    marca linhas suspeitas com base em:
      - empresa diferente da dominante dentro do mesmo domínio
      - empresa não está na lista de empresas prospectadas (se fornecida)
      - empresa aparece apenas uma vez no arquivo
      - nome da empresa diferente da forma mais frequente para aquele nome normalizado
    adc colunas:
      - domain
      - dominant_company
      - dominant_share
      - is_suspect (bool)
      - suspect_reasons (texto)
      - suggested_company (sugestão de correção, se houver)
    """
    df["domain"] = df["Email"].apply(extract_domain)
    df["company_clean"] = df["Company Name"].astype(str).str.strip()
    df["company_norm"] = df["company_clean"].str.lower()
    company_counts = df["company_clean"].value_counts()

    dominant_df = find_dominant_company_per_domain(df)
    df = df.merge(dominant_df, on="domain", how="left")

    canonical_df = find_canonical_company_by_name(df)
    df = df.merge(canonical_df, on="company_norm", how="left")

    suspect_flags = []
    suspect_reasons = []
    suggested_company = []

    for _, row in df.iterrows():
        reasons = []
        suggest = None
        is_suspect = False

        company = row["company_clean"]
        dominant_company = row.get("dominant_company")
        dominant_share = row.get("dominant_share")
        domain = row.get("domain")
        canonical_company = row.get("canonical_company")

        if pd.notna(domain) and pd.notna(dominant_company) and pd.notna(dominant_share):
            if company != dominant_company and dominant_share >= DOMINANT_MIN_SHARE:
                is_suspect = True
                reasons.append(
                    f"Empresa diferente da dominante para o domínio '{domain}' "
                    f"(dominante: '{dominant_company}', share={dominant_share:.0%})"
                )
                suggest = dominant_company

        if prospect_companies:
            if company not in prospect_companies:
                is_suspect = True
                reasons.append("Empresa não está na lista de empresas prospectadas")

        if company_counts.get(company, 0) == 1:
            is_suspect = True
            reasons.append("Empresa aparece apenas uma vez no arquivo")


        if pd.notna(canonical_company) and company != canonical_company:
            is_suspect = True
            reasons.append(
                f"Nome da empresa difere da forma mais frequente no arquivo "
                f"('{canonical_company}')"
            )
            if suggest is None:
                suggest = canonical_company

        suspect_flags.append(is_suspect)
        suspect_reasons.append("; ".join(reasons) if reasons else "")
        suggested_company.append(suggest)

    df["is_suspect"] = suspect_flags
    df["suspect_reasons"] = suspect_reasons
    df["suggested_company"] = suggested_company

    return df

def main():
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument("input_csv")
    parser.add_argument("--empresas-prospectadas", default=None) # -- Opcional
    parser.add_argument("--saida-base", default="apollo")

    args = parser.parse_args()


    input_path = Path(args.input_csv)
    if not input_path.exists():
        raise FileNotFoundError(f"CSV de entrada não encontrado: {input_path}")

    print(f"Lendo {input_path}")
    df = pd.read_csv(input_path)

    required_cols = {"Email", "Company Name"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Faltam colunas obrigatórias no CSV: {missing}")

    prospect_companies = load_prospect_companies(args.empresas_prospectadas)
    if prospect_companies:
        print(f"{len(prospect_companies)} empresas prospectadas carregadas")

    df_marked = mark_suspects(df, prospect_companies)

    base = Path(args.saida_base)

    treated_path = base.with_suffix(".contatos_tratados.csv")
    suspects_path = base.with_suffix(".contatos_suspeitos_isolados.csv")

    df_marked.to_csv(treated_path, index=False, encoding="utf-8-sig")
    df_marked[df_marked["is_suspect"]].to_csv(
        suspects_path, index=False, encoding="utf-8-sig"
    )
    
        # --- gerar pac.csv ---
    df_pac = df_marked.copy()

    df_pac["Company Name"] = df_pac.apply(
        lambda r: r["suggested_company"] if pd.notna(r["suggested_company"]) and r["suggested_company"] != "" else r["Company Name"],
        axis=1
    )

    cols_to_remove = [
        "domain", "company_clean", "company_norm",
        "dominant_company", "dominant_share", "canonical_company",
        "is_suspect", "suspect_reasons", "suggested_company"
    ]

    df_pac = df_pac.drop(columns=[c for c in cols_to_remove if c in df_pac.columns])

    pac_path = base.with_suffix("pac.csv")
    df_pac.to_csv(pac_path, index=False, encoding="utf-8-sig")


    print(f"deu certo boy")

if __name__ == "__main__":
    main()
