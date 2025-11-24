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
	@echo "  make test-all         - Полное тестирование на всех датасетах"
	@echo "  make test-all-n n=10  - Запустить test-all N раз"
	@echo ""
	@echo "=== Docker ==="
	@echo "  make docker-up        - Запуск всех сервисов Docker"
	@echo "  make docker-down      - Остановка Docker"
	@echo ""
	@echo "=== Просмотр результатов ==="
	@echo "  make view n=filename  - Просмотр результатов"
	@echo ""
	@echo "Используйте: make <команда>"

cleanup-db:
	@echo "🧹 Очистка баз данных..."
	. venv/bin/activate && python scripts/cleanup_databases.py
	sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

test-small:
	@echo "🔬 Тестирование на SMALL датасете..."
	make cleanup-db
	. venv/bin/activate && python scripts/dataset_manager.py small
	make cleanup-db

test-medium:
	@echo "🔬 Тестирование на MEDIUM датасете..."
	make cleanup-db
	. venv/bin/activate && python scripts/dataset_manager.py medium
	make cleanup-db

test-large:
	@echo "🔬 Тестирование на LARGE датасете..."
	make cleanup-db
	. venv/bin/activate && python scripts/dataset_manager.py large
	make cleanup-db

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

test-all-n:
	@mkdir -p results/iterations
	@echo "🚀 Запуск $(n) итераций test-all..."
	@for i in $$(seq 1 $(n)); do \
		echo "🎯 Итерация $$i/$(n)"; \
		$(MAKE) test-all 2>&1 | tee "results/iterations/iteration_$$i.log"; \
		echo "=========================================="; \
	done
	@echo "✅ $(n) итераций завершены! Логи в results/iterations/"
