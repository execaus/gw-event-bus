.PHONY: tag
tag:
	@CURRENT_TAG=$$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"); \
	echo "Текущий тег: $$CURRENT_TAG"; \
	IFS='.' read -r MAJOR MINOR PATCH <<< "$${CURRENT_TAG#v}"; \
	echo "Что повышаем?"; \
	echo "  1) major"; \
	echo "  2) minor"; \
	echo "  3) patch (по умолчанию)"; \
	read -p "Выбор [3]: " CHOICE; \
	CHOICE=$${CHOICE:-3}; \
	if [ "$$CHOICE" = "1" ]; then \
		MAJOR=$$((MAJOR + 1)); MINOR=0; PATCH=0; \
	elif [ "$$CHOICE" = "2" ]; then \
		MINOR=$$((MINOR + 1)); PATCH=0; \
	elif [ "$$CHOICE" = "3" ]; then \
		PATCH=$$((PATCH + 1)); \
	else \
		echo "Неверный выбор: $$CHOICE"; exit 1; \
	fi; \
	NEW_TAG="v$$MAJOR.$$MINOR.$$PATCH"; \
	git tag -a "$$NEW_TAG" -m "Release $$NEW_TAG"; \
	git push origin "$$NEW_TAG"; \
	echo "Тег $$NEW_TAG успешно создан и отправлен."