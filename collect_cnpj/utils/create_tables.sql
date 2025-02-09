CREATE TABLE IF NOT EXISTS empresas (
    cnpj TEXT,
    razao_social TEXT,
    natureza_juridica INT,
    qualificacao_responsavel INT,
    capital_social TEXT,
    cod_porte TEXT,
    data_ref DATE,
    data_execution DATE
);

CREATE TABLE IF NOT EXISTS socios (
    cnpj TEXT,
    tipo_socio INT,
    nome_socio TEXT,
    documento_socio TEXT,
    cod_qualificacao_socio TEXT,
    data_ref DATE,
    data_execution DATE
);

CREATE TABLE IF NOT EXISTS result (
    cnpj TEXT,
    qtde_socios INT,
    flag_socio_estrangeiro BOOLEAN,
    doc_alvo BOOLEAN,
    data_ref DATE,
    data_execution DATE
);

CREATE TABLE IF NOT EXISTS silver (
    cnpj TEXT,
    cod_porte INT,
    flag_socio_estrangeiro BOOLEAN,
    socio_id TEXT,
    data_ref DATE,
    data_execution DATE
);