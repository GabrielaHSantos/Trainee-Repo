
---obs: Como explicado no read.me eu inseri os dados de um bd pra outro pq eu preferi criar um bd e transferir.Então, o bd antigo se chama concessionaria2 conforme vc vai perceber na hora de chegar na inserção de dados.

-----Criando tabelas pra normalização da ta bela unica-----

--- Clientes

CREATE TABLE Clientes (
    idCliente INT PRIMARY KEY,
    nomeCliente VARCHAR(100) not null,
    cidadeCliente VARCHAR(50) not null,
    estadoCliente VARCHAR(50) not null,
    paisCliente VARCHAR(50) not null
);

---Vendedor

CREATE TABLE Vendedor (
    idVendedor INT PRIMARY KEY,
    nomeVendedor VARCHAR(100) not null,
    sexoVendedor smallint not null,
    estadoVendedor VARCHAR(50) not null
);


--- Combustivel

CREATE TABLE Combustivel (
    idCombustivel INT PRIMARY KEY,
    tipoCombustivel VARCHAR(50) not null
);


--- Carros

CREATE TABLE Carros (
    idCarro INT PRIMARY KEY,
    marcaCarro VARCHAR(50) not null,
    modeloCarro VARCHAR(50) not null,
    anoCarro INT not null,
    kmCarro INT not null,
    classiCarro VARCHAR(50) not null,
    idCombustivel INT,
    FOREIGN KEY (idCombustivel) REFERENCES Combustivel(idCombustivel)
);

--- DatasLocacao

CREATE TABLE DatasLocacao (
    id_data INT PRIMARY KEY,  
    dataLocacao DATE,               
    horaLocacao TIME,            
    dataEntrega DATE,                
    horaEntrega TIME                 
);


---Locacoes

CREATE TABLE Locacoes (
    idLocacao INT PRIMARY KEY,
    idCliente INT,
    idCarro INT,
    idVendedor INT,
    id_data INT, 
    qtdDiaria INT,
    vlrDiaria DECIMAL(10, 2),
    FOREIGN KEY (idCliente) REFERENCES Clientes(idCliente),
    FOREIGN KEY (idCarro) REFERENCES Carros(idCarro),
    FOREIGN KEY (idVendedor) REFERENCES Vendedor(idVendedor),
    FOREIGN KEY (id_data) REFERENCES DatasLocacao(id_data) 
);


-----Trazendo dados da outra tabela----- (necessario renomear a db pra concessionaria2 -----

--- Clientes


INSERT INTO Clientes (idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente)
SELECT DISTINCT idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente
FROM concessionaria2.tb_locacao;


---Vendedores

insert into Vendedores (idVendedor, nomeVendedor, sexoVendedor, estadoVendedor)
select distinct idVendedor, nomeVendedor, sexoVendedor, estadoVendedor
From concessionaria2.tb_locacao;


---Combustiveis

INSERT INTO Combustiveis (idCombustivel, tipoCombustivel)
SELECT DISTINCT idcombustivel, tipoCombustivel
From concessionaria2.tb_locacao;


--- Carros

insert or ignore into Carros (idCarro, marcaCarro, modeloCarro, anoCarro, kmCarro, classiCarro,idCombustivel)
select distinct idCarro, marcaCarro, modeloCarro, anoCarro, kmCarro, classiCarro, idcombustivel
From concessionaria2.tb_locacao;


--- DatasLocacao

INSERT INTO DatasLocacao (id_data, dataLocacao, horaLocacao, dataEntrega, horaEntrega)
SELECT 
    idLocacao AS id_data,
    DATE(
        substr(dataLocacao, 1, 4) || '-' ||   -- Ano
        substr(dataLocacao, 5, 2) || '-' ||   -- Mês
        substr(dataLocacao, 7, 2)             -- Dia
    ) AS dataLocacao,
    TIME(
        printf('%02d:%02d', 
            CAST(substr(horaLocacao, 1, instr(horaLocacao, ':')-1) AS INTEGER), 
            CAST(substr(horaLocacao, instr(horaLocacao, ':')+1) AS INTEGER)
        )
    ) AS horaLocacao,
    DATE(
        substr(dataEntrega, 1, 4) || '-' ||   -- Ano
        substr(dataEntrega, 5, 2) || '-' ||   -- Mês
        substr(dataEntrega, 7, 2)             -- Dia
    ) AS dataEntrega,
    TIME(
        printf('%02d:%02d', 
            CAST(substr(horaEntrega, 1, instr(horaEntrega, ':')-1) AS INTEGER), 
            CAST(substr(horaEntrega, instr(horaEntrega, ':')+1) AS INTEGER)
        )
    ) AS horaEntrega
FROM concessionaria2.tb_locacao;


---Locacoes

INSERT INTO Locacoes (
    idLocacao, 
    idCliente, 
    idCarro, 
    idVendedor, 
    id_data, 
    qtdDiaria, 
    vlrDiaria
)
SELECT 
    idLocacao,  
    idCliente,
    idCarro,
    idVendedor,
    idLocacao AS id_data,  
    qtdDiaria,
    vlrDiaria
FROM 
    concessionaria2.tb_locacao;

-----verificando as tabelas relacionais-----

SELECT*FROM Clientes;
SELECT*FROM Carros;
SELECT*FROM Vendedor;
SELECT*FROM Combustiveis;
SELECT*FROM DatasLocacao;
SELECT*FROM Locacoes;

-----Criando Views-----

--- Dim Clientes

CREATE VIEW Dim_Clientes AS
SELECT Distinct
    idCliente AS Codigo,
    nomeCliente AS Cliente,
    cidadeCliente AS Cidade,
    estadoCliente AS Estado,
    paisCliente AS Pais
FROM Clientes;


--- Dim Vendedor

CREATE VIEW Dim_Vendedor AS
SELECT 
    idVendedor AS Codigo,
    nomeVendedor AS Vendedor,
    CASE sexoVendedor
        WHEN 0 THEN 'Masculino'
        WHEN 1 THEN 'Feminino'
        ELSE 'Outro'
    END AS Genero,
    estadoVendedor AS Estado
FROM Vendedor;

--- Dim Carros

CREATE VIEW Dim_Carros AS
SELECT 
    idCarro AS Codigo,
    marcaCarro AS Marca,
    modeloCarro AS Modelo,
    anoCarro AS Ano,
    kmCarro AS Quilometragem,
    classiCarro AS Classi,
    (SELECT tipoCombustivel 
     FROM Combustiveis 
     WHERE Combustiveis.idCombustivel = Carros.idCombustivel) AS TipoCombustivel
FROM Carros;

--- Dim Datas

CREATE VIEW Dim_Datas AS
SELECT 
    id_data AS id_data,
    dataLocacao AS data_locacao,
    strftime('%Y', dataLocacao) AS ano_locacao,
    strftime('%m', dataLocacao) AS mes_locacao,
    strftime('%d', dataLocacao) AS dia_locacao,
    strftime('%w', dataLocacao) AS dia_semana_locacao,
    horaLocacao AS hora_locacao,
    dataEntrega AS data_entrega,
    strftime('%Y', dataEntrega) AS ano_entrega,
    strftime('%m', dataEntrega) AS mes_entrega,
    strftime('%d', dataEntrega) AS dia_entrega,
    strftime('%w', dataEntrega) AS dia_semana_entrega,
    horaEntrega AS hora_entrega
FROM DatasLocacao;


--- Fato Locacoes

CREATE VIEW Fato_Locacoes AS
SELECT 
    L.idLocacao AS Codigo_Locacao,
    L.idCliente AS Codigo_Cliente,
    L.idVendedor AS Codigo_Vendedor,
    L.idCarro AS Codigo_Carro,
    L.id_data AS Codigo_Data,
    L.qtdDiaria AS Quantidade_Diarias,
    L.vlrDiaria AS Valor_Diaria,
    (L.qtdDiaria * L.vlrDiaria) AS Valor_Total
FROM 
    Locacoes L;

-----verificando as tabelas dimensionais-----

SELECT*FROM Dim_Clientes;

SELECT*FROM Dim_Vendedor;

SELECT*FROM Dim_Carros;

SELECT*FROM Dim_Datas;

SELECT*FROM Fato_Locacoes;