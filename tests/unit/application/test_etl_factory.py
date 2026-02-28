# tests/unit/application/test_etl_factory.py
"""
Testes para ETLFactory.
Verifica criação de serviços ETL por entidade com injeção de dependências.
"""

import pytest
from unittest import mock

from src.application.etl_factory import ETLFactory
from src.application.etl_service import ETLService


class TestETLFactory:
    """Testes para factory de ETL seguindo padrão Strategy."""
    
    def test_create_rockets_service_success(self):
        """Deve criar serviço configurado para rockets."""
        # Mockar as classes no registry da factory
        mock_extractor_instance = mock.Mock()
        mock_transformer_instance = mock.Mock()
        mock_silver_instance = mock.Mock()
        
        original_strategies = ETLFactory._strategies.copy()
        
        try:
            # Substituir strategies por mocks
            ETLFactory._strategies["rockets"] = {
                "extractor": mock.Mock(return_value=mock_extractor_instance),
                "transformer": mock.Mock(return_value=mock_transformer_instance),
                "silver_loader": mock.Mock(return_value=mock_silver_instance),
            }
            
            with mock.patch('src.application.etl_factory.BronzeLoader') as mock_bronze, \
                 mock.patch('src.application.etl_factory.WatermarkManager'), \
                 mock.patch('src.application.etl_factory.PipelineMetrics'), \
                 mock.patch('src.application.etl_factory.SlackNotifier'), \
                 mock.patch('src.application.etl_factory.SchemaValidator'):
                
                service = ETLFactory.create(
                    entity="rockets",
                    incremental=False,
                    real_api=True
                )
                
                # Verificar
                assert isinstance(service, ETLService)
                assert service.entity == "rockets"
                assert service.incremental is False
                assert service.real_api is True
                
                # Verificar que componentes específicos foram criados
                ETLFactory._strategies["rockets"]["extractor"].assert_called_once()
                ETLFactory._strategies["rockets"]["transformer"].assert_called_once()
                ETLFactory._strategies["rockets"]["silver_loader"].assert_called_once()
                
        finally:
            # Restaurar strategies originais
            ETLFactory._strategies = original_strategies
    
    def test_create_launches_service_success(self):
        """Deve criar serviço configurado para launches."""
        mock_extractor_instance = mock.Mock()
        mock_transformer_instance = mock.Mock()
        mock_silver_instance = mock.Mock()
        
        original_strategies = ETLFactory._strategies.copy()
        
        try:
            ETLFactory._strategies["launches"] = {
                "extractor": mock.Mock(return_value=mock_extractor_instance),
                "transformer": mock.Mock(return_value=mock_transformer_instance),
                "silver_loader": mock.Mock(return_value=mock_silver_instance),
            }
            
            with mock.patch('src.application.etl_factory.BronzeLoader'), \
                 mock.patch('src.application.etl_factory.WatermarkManager'), \
                 mock.patch('src.application.etl_factory.PipelineMetrics'), \
                 mock.patch('src.application.etl_factory.SlackNotifier'), \
                 mock.patch('src.application.etl_factory.SchemaValidator'):
                
                service = ETLFactory.create(
                    entity="launches",
                    incremental=True,
                    real_api=False
                )
                
                assert isinstance(service, ETLService)
                assert service.entity == "launches"
                assert service.incremental is True
                assert service.real_api is False
                
        finally:
            ETLFactory._strategies = original_strategies
    
    def test_create_unsupported_entity_raises_value_error(self):
        """Deve lançar ValueError para entidade não suportada."""
        with pytest.raises(ValueError) as exc_info:
            ETLFactory.create("unknown_entity", incremental=False, real_api=True)
        
        assert "unknown_entity" in str(exc_info.value)
        assert "not supported" in str(exc_info.value).lower()
    
    def test_create_error_message_lists_available_entities(self):
        """Mensagem de erro deve listar entidades disponíveis."""
        with pytest.raises(ValueError, match="rockets.*launches"):
            ETLFactory.create("invalid", incremental=False, real_api=True)
    
    def test_strategies_registry_is_dict(self):
        """Registry deve ser um dicionário."""
        assert isinstance(ETLFactory._strategies, dict)
        assert "rockets" in ETLFactory._strategies
        assert "launches" in ETLFactory._strategies
    
    def test_strategies_have_required_keys(self):
        """Cada estratégia deve ter extractor, transformer e silver_loader."""
        for entity, strategy in ETLFactory._strategies.items():
            assert "extractor" in strategy, f"{entity} missing extractor"
            assert "transformer" in strategy, f"{entity} missing transformer"
            assert "silver_loader" in strategy, f"{entity} missing silver_loader"
    
    def test_register_new_entity_success(self):
        """Deve permitir registrar nova entidade dinamicamente."""
        mock_extractor = mock.Mock
        mock_transformer = mock.Mock
        mock_loader_factory = mock.Mock(return_value=mock.Mock())
        
        # Registrar
        ETLFactory.register_entity(
            entity="capsules",
            extractor_class=mock_extractor,
            transformer_class=mock_transformer,
            silver_loader_factory=mock_loader_factory,
        )
        
        # Verificar registro
        assert "capsules" in ETLFactory._strategies
        assert ETLFactory._strategies["capsules"]["extractor"] == mock_extractor
        assert ETLFactory._strategies["capsules"]["transformer"] == mock_transformer
        assert ETLFactory._strategies["capsules"]["silver_loader"] == mock_loader_factory
        
        # Limpar após teste
        del ETLFactory._strategies["capsules"]
    
    def test_registered_entity_can_be_created(self):
        """Entidade registrada deve poder ser instanciada."""
        # Setup
        mock_extractor_instance = mock.Mock()
        mock_transformer_instance = mock.Mock()
        mock_silver_instance = mock.Mock()
        
        MockExtractor = mock.Mock(return_value=mock_extractor_instance)
        MockTransformer = mock.Mock(return_value=mock_transformer_instance)
        MockSilverFactory = mock.Mock(return_value=mock_silver_instance)
        
        ETLFactory.register_entity(
            entity="test_entity",
            extractor_class=MockExtractor,
            transformer_class=MockTransformer,
            silver_loader_factory=MockSilverFactory,
        )
        
        # Execute com mocks para outros componentes
        with mock.patch('src.application.etl_factory.BronzeLoader'), \
             mock.patch('src.application.etl_factory.WatermarkManager'), \
             mock.patch('src.application.etl_factory.PipelineMetrics'), \
             mock.patch('src.application.etl_factory.SlackNotifier'), \
             mock.patch('src.application.etl_factory.SchemaValidator'):
            
            service = ETLFactory.create("test_entity", incremental=False, real_api=True)
            
            assert service.entity == "test_entity"
            MockExtractor.assert_called_once()
            MockTransformer.assert_called_once()
            MockSilverFactory.assert_called_once()
        
        # Cleanup
        del ETLFactory._strategies["test_entity"]
    
    def test_silver_loader_factory_is_callable(self):
        """silver_loader deve ser uma factory (callable)."""
        for entity, strategy in ETLFactory._strategies.items():
            assert callable(strategy["silver_loader"]), \
                f"{entity} silver_loader is not callable"
    
    def test_extractor_and_transformer_are_classes(self):
        """extractor e transformer devem ser classes (não instâncias)."""
        for entity, strategy in ETLFactory._strategies.items():
            assert isinstance(strategy["extractor"], type), \
                f"{entity} extractor is not a class"
            assert isinstance(strategy["transformer"], type), \
                f"{entity} transformer is not a class"
    
    def test_create_injects_all_dependencies(self):
        """Deve injetar todas as dependências no ETLService."""
        # Criar mocks para todos os componentes
        mock_extractor = mock.Mock()
        mock_transformer = mock.Mock()
        mock_silver = mock.Mock()
        mock_bronze = mock.Mock()
        mock_watermark = mock.Mock()
        mock_metrics = mock.Mock()
        mock_notifier = mock.Mock()
        mock_validator = mock.Mock()
        
        original_strategies = ETLFactory._strategies.copy()
        
        try:
            # Substituir strategies
            ETLFactory._strategies["rockets"] = {
                "extractor": mock.Mock(return_value=mock_extractor),
                "transformer": mock.Mock(return_value=mock_transformer),
                "silver_loader": mock.Mock(return_value=mock_silver),
            }
            
            with mock.patch('src.application.etl_factory.BronzeLoader', return_value=mock_bronze), \
                 mock.patch('src.application.etl_factory.WatermarkManager', return_value=mock_watermark), \
                 mock.patch('src.application.etl_factory.PipelineMetrics', return_value=mock_metrics), \
                 mock.patch('src.application.etl_factory.SlackNotifier', return_value=mock_notifier), \
                 mock.patch('src.application.etl_factory.SchemaValidator', return_value=mock_validator):
                
                service = ETLFactory.create("rockets", incremental=True, real_api=False)
                
                # Verificar que todas as dependências foram injetadas
                assert service.extractor is mock_extractor
                assert service.transformer is mock_transformer
                assert service.silver_loader is mock_silver
                assert service.bronze_loader is mock_bronze
                assert service.watermark is mock_watermark
                assert service.metrics is mock_metrics
                assert service.notifier is mock_notifier
                assert service.schema_validator is mock_validator
                
        finally:
            ETLFactory._strategies = original_strategies