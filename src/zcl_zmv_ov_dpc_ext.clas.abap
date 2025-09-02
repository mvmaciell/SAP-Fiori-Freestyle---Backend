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


  method OVCABSET_GET_ENTITYSET.
  endmethod.


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


  method OVITEMSET_GET_ENTITYSET.
  endmethod.


  method OVITEMSET_UPDATE_ENTITY.
  endmethod.
ENDCLASS.
