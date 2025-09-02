class ZCL_ZMV_OV_DPC_EXT definition
  public
  inheriting from ZCL_ZMV_OV_DPC
  create public .

public section.
protected section.

  methods MENSAGEMSET_CREATE_ENTITY
    redefinition .
  methods MENSAGEMSET_DELETE_ENTITY
    redefinition .
  methods MENSAGEMSET_GET_ENTITY
    redefinition .
  methods MENSAGEMSET_GET_ENTITYSET
    redefinition .
  methods MENSAGEMSET_UPDATE_ENTITY
    redefinition .
  methods OVCABSET_CREATE_ENTITY
    redefinition .
  methods OVCABSET_DELETE_ENTITY
    redefinition .
  methods OVCABSET_GET_ENTITY
    redefinition .
  methods OVCABSET_GET_ENTITYSET
    redefinition .
  methods OVCABSET_UPDATE_ENTITY
    redefinition .
  methods OVITEMSET_CREATE_ENTITY
    redefinition .
  methods OVITEMSET_DELETE_ENTITY
    redefinition .
  methods OVITEMSET_GET_ENTITY
    redefinition .
  methods OVITEMSET_GET_ENTITYSET
    redefinition .
  methods OVITEMSET_UPDATE_ENTITY
    redefinition .
private section.
ENDCLASS.



CLASS ZCL_ZMV_OV_DPC_EXT IMPLEMENTATION.


  method MENSAGEMSET_CREATE_ENTITY.
  endmethod.


  method MENSAGEMSET_DELETE_ENTITY.
  endmethod.


  method MENSAGEMSET_GET_ENTITY.
  endmethod.


  method MENSAGEMSET_GET_ENTITYSET.
  endmethod.


  method MENSAGEMSET_UPDATE_ENTITY.
  endmethod.


  METHOD ovcabset_create_entity.

    " Último ID gerado
    DATA: ld_lastid TYPE int4,
          ls_cab    TYPE zjv_ovcab.

    " Container padrão de mensagens do SAP Gateway (OData)
    DATA(lo_msg) = me->/iwbep/if_mgw_conv_srv_runtime~get_message_container(  ).

    " Lê o payload JSON enviado no POST e preenche er_entity (estrutura do EntityType)
    io_data_provider->read_entry_data( IMPORTING es_data = er_entity ).

    " Copia campos com mesmo nome de er_entity -> ls_cab (estrutura da tabela Z)
    MOVE-CORRESPONDING er_entity TO ls_cab.

    ls_cab-criacao_data = sy-datum.
    ls_cab-criacao_hora = sy-uzeit.
    ls_cab-criacao_usuario = sy-uname.

    " Preenche campos técnicos de criação com data, hora e usuário do servidor
    SELECT SINGLE MAX( ordemid ) INTO ld_lastid FROM zjv_ovcab.

    " Gera ordemid de forma ingênua pegando o MAX atual (RACE CONDITION!)
    ls_cab-ordemid = ld_lastid + 1.

    " Grava diretamente na tabela Z (sem lock/autorização/validação)
    INSERT zjv_ovcab FROM ls_cab.

    " Se falhar, devolve erro de negócio no padrão Gateway
    IF sy-subrc <> 0.
      lo_msg->add_message_text_only(
      EXPORTING
        iv_msg_type = 'E'
        iv_msg_text = 'Erro ao inserir ordem' ).

      RAISE EXCEPTION TYPE /iwbep/cx_mgw_busi_exception
        EXPORTING
          message_container = lo_msg.

    ENDIF.

    " Reflete dados gravados de volta ao Entity de resposta
    MOVE-CORRESPONDING ls_cab TO er_entity.

    " Converte data/hora locais em timestamp (fuso = sy-zonlo) para o campo OData
    CONVERT
        DATE ls_cab-criacao_data
        TIME ls_cab-criacao_hora
        INTO TIME STAMP er_entity-datacriacao
        TIME ZONE sy-zonlo.

  ENDMETHOD.


  method OVCABSET_DELETE_ENTITY.
  endmethod.


  method OVCABSET_GET_ENTITY.
  endmethod.


  METHOD ovcabset_get_entityset.

    " Tabelas e estruturas de trabalho
    DATA: lt_cab TYPE TABLE OF zjv_ovcab.     "buffer para linhas lidas do cabeçalho (tabela Z)
    DATA: ls_cab TYPE zjv_ovcab.              "work area para percorrer lt_cab
    DATA: ls_entityset LIKE LINE OF et_entityset. "linha do EntitySet OData de resposta

    " Suporte à ordenação dinâmica (ORDER BY)
    DATA: lt_orderby TYPE STANDARD TABLE OF string. "componentes 'CAMPO DIREÇÃO'
    DATA: ld_orderby TYPE string.                   "cláusula ORDER BY final

    " Normaliza a lista de ordenação recebida do framework OData (it_order)
    LOOP AT it_order INTO DATA(ls_order).
      TRANSLATE ls_order-property TO UPPER CASE. "padroniza nome do campo
      TRANSLATE ls_order-order    TO UPPER CASE. "padroniza direção (ASC/DESC)
      IF ls_order-order = 'DESC'.
        ls_order-order = 'DESCENDING'.   "palavra-chave esperada no Open SQL
      ELSE.
        ls_order-order = 'ASCENDING'.
      ENDIF.

      "Monta um item 'CAMPO DIREÇÃO' para depois concatenar tudo
      APPEND |{ ls_order-property } { ls_order-order }| TO lt_orderby.

    ENDLOOP.

    " Concatena os itens em uma única string, separando por espaço
    CONCATENATE LINES OF lt_orderby INTO ld_orderby SEPARATED BY ' '.

    " Caso o consumidor não mande $orderby, define um padrão
    IF ld_orderby = ''.
      ld_orderby = 'OrdemId ASCENDING'.
    ENDIF.

    " Seleção de dados com:
    " - filtro dinâmico (iv_filter_string) vindo do framework OData
    " - ordenação dinâmica (ld_orderby)
    " - paginação (TOP/SKIP) de acordo com is_paging
    SELECT *
      FROM zjv_ovcab
      WHERE (iv_filter_string)     "filtro dinâmico pré-montado
      ORDER BY (ld_orderby)        "ordenar pelos campos/direções solicitados
      INTO TABLE @lt_cab
      UP TO   @is_paging-top ROWS  "limite ($top)
      OFFSET  @is_paging-skip.     "deslocamento ($skip)

    " Transforma as linhas da tabela Z no formato do EntitySet OData
    LOOP AT lt_cab INTO ls_cab.
      CLEAR ls_entityset.
      MOVE-CORRESPONDING ls_cab TO ls_entityset.  "mapa campos homônimos

      " Ajusta campos de exibição: quem criou (alias amigável)
      ls_entityset-criadopor = ls_cab-criacao_usuario.

      " Converte data/hora locais em timestamp para o campo OData
      CONVERT DATE ls_cab-criacao_data
              TIME ls_cab-criacao_hora
        INTO TIME STAMP ls_entityset-datacriacao
        TIME ZONE sy-zonlo.

      " Adiciona a linha ao conjunto de resposta
      APPEND ls_entityset TO et_entityset.
    ENDLOOP.

  ENDMETHOD.


  method OVCABSET_UPDATE_ENTITY.
  endmethod.


  method OVITEMSET_CREATE_ENTITY.

    " Declara uma estrutura local correspondente à tabela Z de itens
    DATA: ls_item    TYPE zjv_ovitem.

    " Recupera o container de mensagens do Gateway (usado para devolver erros ao consumidor OData)
    DATA(lo_msg) = me->/iwbep/if_mgw_conv_srv_runtime~get_message_container(  ).

    " Lê os dados enviados no payload do POST (JSON -> estrutura ABAP er_entity)
    io_data_provider->read_entry_data( IMPORTING es_data = er_entity ).

    " Copia os campos recebidos (com nomes equivalentes) para a estrutura da tabela Z
    MOVE-CORRESPONDING er_entity TO ls_item.

    " Regra de geração de ID do item:
    IF er_entity-itemid = 0.

      " Busca o maior itemid já existente para a mesma ordemid
      SELECT SINGLE MAX( itemid ) INTO er_entity-itemid FROM zjv_ovitem WHERE ordemid = er_entity-ordemid.

      " Incrementa em +1 para gerar o próximo item
      er_entity-itemid = er_entity-itemid + 1.

    ENDIF.

    " Insere diretamente o registro na tabela Z
    INSERT zjv_ovitem FROM ls_item.

    " Caso ocorra erro no INSERT, registra mensagem de erro no container e dispara exceção de negócio OData
    IF sy-subrc <> 0.
      lo_msg->add_message_text_only(
      EXPORTING
        iv_msg_type = 'E'
        iv_msg_text = 'Erro ao inserir item' ).

      RAISE EXCEPTION TYPE /iwbep/cx_mgw_busi_exception
        EXPORTING
          message_container = lo_msg.

    ENDIF.

  endmethod.


  method OVITEMSET_DELETE_ENTITY.
  endmethod.


  method OVITEMSET_GET_ENTITY.
  endmethod.


  METHOD ovitemset_get_entityset.

    " --- Declarações de trabalho ---
    DATA: ld_ordemid        TYPE int4.                   " valor numérico do parâmetro-chave 'OrdemId'
    DATA: lt_ordemid_range  TYPE RANGE OF int4.          " range-table (SIGN/OPTION/LOW/HIGH) para usar no WHERE ... IN
    DATA: ls_ordemid_range  LIKE LINE OF lt_ordemid_range. " linha do range para preencher
    DATA: ls_key_tab        LIKE LINE OF it_key_tab.     " par (name,value) vindo do Gateway (chave da entidade)

    " Procura no it_key_tab a chave 'OrdemId' (típico de GET_ENTITY / navegação)
    READ TABLE it_key_tab INTO ls_key_tab WITH KEY name = 'OrdemId'.
    IF sy-subrc = 0.
      " Converte o valor textual do par para o tipo alvo (aqui int4)
      ld_ordemid = ls_key_tab-value.

      " Zera a linha de range e monta um intervalo do tipo 'incluir' e 'igual'
      CLEAR ls_ordemid_range.
      ls_ordemid_range-sign   = 'I'.     " Include
      ls_ordemid_range-option = 'EQ'.    " Igual
      ls_ordemid_range-low    = ld_ordemid. " Valor alvo
      " Adiciona no range a ser usado no WHERE ... IN
      APPEND ls_ordemid_range TO lt_ordemid_range.
    ENDIF.

    " Lê itens da ZJV_OVITEM filtrando pelo range (WHERE ordemid IN lt_ordemid_range)
    " e preenche o entityset mapeando campos homônimos
    SELECT *
      INTO CORRESPONDING FIELDS OF TABLE et_entityset
      FROM zjv_ovitem
      WHERE ordemid IN lt_ordemid_range.

  ENDMETHOD.


  method OVITEMSET_UPDATE_ENTITY.
  endmethod.
ENDCLASS.
