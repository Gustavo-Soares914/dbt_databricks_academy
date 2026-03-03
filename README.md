# Adventure Works Analytics

Este projeto implementa um pipeline analítico completo utilizando dados do Adventure Works, contemplando:

- Modelagem dimensional em star schema
- Transformações com dbt
- Camada semântica para BI
- Dashboard interativo no Power BI

O objetivo é permitir análises de vendas por produto, cliente, território, motivo de venda e período.

# Arquitetura

O projeto foi estruturado da seguinte maneira utilizabndo o DBT Cloud em conjunto com o Databricks.

Raw → Staging → Intermediate → Mart → Power BI

- Staging: padronização e limpeza
- Intermediate: regras de negócio e joins
- Mart: modelo dimensional final
- BI: visualização e métricas

# Modelagem de dados

O modelo foi estruturado em star schema.

### Fato
- fact_sales_adv
  - granularidade: item do pedido

### Dimensões
- dim_customer_adv
- dim_products_adv
- dim_territory_adv
- dim_date_adv
- dim_credit_card_adv
- dim_status_adv
- dim_sales_reason

### Bridge
- bridge_order_sales_reason
  - resolve relacionamento N:N entre pedidos e motivos de venda

# Testes
No DBT foram aplicados testes de unique, not null e relationships nas tabelas .yml, estando presentes nas camadas Staging e Marts. Os testes foram aplicados principalmente nas chaves primárias, secundárias e surrogate keys. Foi identificado que existem alguns atributos na base de dados de origem que possuem preenchimento opcional, por esse motivo os testes de "not null" para esses atributos foram desativados. 

# Visualização no BI
Para uma melhor visualizações dos resultados foi estruturado um dashboard no Power Bi onde é possivel realizar as análises dos resultados obtidos após a modelagem.

### Link Power BI
- link: https://app.powerbi.com/view?r=eyJrIjoiMzk5OGE5NjAtNDM0NC00NmYxLThlYWYtY2UyMDYyZmYyNTZhIiwidCI6ImQ0NjNhM2ZhLTliYmUtNDk1OS1iMGYxLTYzYjkzYjA0MzA0ZCJ9
 
