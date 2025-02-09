CREATE TABLE IF NOT EXISTS empresas (
    cnpj TEXT,
    razao_social TEXT,
    natureza_juridica INT,
    qualificacao_responsavel INT,
    capital_social TEXT,
    cod_porte TEXT
);

CREATE TABLE IF NOT EXISTS socios (
    cnpj TEXT,
    tipo_socio INT,
    nome_socio TEXT,
    documento_socio TEXT,
    cod_qualificacao_socio TEXT
);

CREATE TABLE IF NOT EXISTS result (
    cnpj TEXT,
    qtde_socios INT,
    flag_socio_estrangeiro BOOLEAN,
    doc_alvo BOOLEAN
);

CREATE TABLE IF NOT EXISTS silver (
    cnpj TEXT,
    cod_porte INT,
    flag_socio_estrangeiro BOOLEAN,
    socio_id TEXT
);
