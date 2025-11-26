.PHONY: help cleanup-db test-small test-medium test-large test-all clear-results docker-up docker-down view test-all-n

help:
	@echo "Доступные команды:"
	@echo ""
	@echo "=== Основные ==="
	@echo "  make cleanup-db       - Очистка баз данных + drop caches"
	@echo "  make clear-results    - Полная очистка результатов"
	@echo ""
	@echo "=== Тестирование ==="
	@echo "  make test-small       - Тестирование на SMALL датасете"
	@echo "  make test-medium      - Тестирование на MEDIUM датасете"
	@echo "  make test-large       - Тестирование на LARGE датасете"
	@echo ""
	@echo "=== Docker ==="
	@echo "  make docker-up        - Запуск всех сервисов Docker"
	@echo "  make docker-down      - Остановка Docker"
	@echo ""
	@echo "=== Просмотр результатов ==="
	@echo "  make view n=files_num  - Просмотр результатов"
	@echo ""
	@echo "Используйте: make <команда>"

# cleanup-db:
# 	@echo "🧹 Очистка баз данных..."
# 	. venv/bin/activate && python scripts/cleanup_databases.py
# 	sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

test-small:
	@echo "🔬 Тестирование на SMALL датасете..."
	. venv/bin/activate && python scripts/dataset_manager.py small

test-small:
	@echo "🔬 Тестирование на SMALL датасете..."
	. venv/bin/activate && python scripts/dataset_manager.py small

test-medium:
	@echo "🔬 Тестирование на MEDIUM датасете..."
	. venv/bin/activate && python scripts/dataset_manager.py medium

test-large:
	@echo "🔬 Тестирование на LARGE датасете..."
	. venv/bin/activate && python scripts/dataset_manager.py large

test-large:
	@echo "🔬 Тестирование на всех датасетах..."
	. venv/bin/activate && python scripts/dataset_manager.py all

clear-results:
	@echo "🧹 Очистка результатов..."
	rm -rf results/*
	rm -rf data/medium data/large
	@echo "✅ Результаты очищены"
	
docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

view:
	. venv/bin/activate && python scripts/view_results.py $(n)
