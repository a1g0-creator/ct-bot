import logging
from telegram.constants import ParseMode
from telegram.error import BadRequest
import asyncio
import traceback

logger = logging.getLogger(__name__)

def patch_telegram_methods():
    """
    Применяет патч к методам telegram.Message для безопасной отправки сообщений.
    Автоматически обрабатывает ошибки форматирования и отправляет сообщения без форматирования в случае ошибки.
    """
    from telegram import Message
    original_reply_text = Message.reply_text
    
    async def patched_reply_text(self, text, parse_mode=None, **kwargs):
        """Безопасная версия reply_text с обработкой ошибок форматирования"""
        try:
            # Сначала пробуем отправить сообщение как есть
            return await original_reply_text(self, text, parse_mode=parse_mode, **kwargs)
        except BadRequest as e:
            # Если возникла ошибка форматирования, пробуем отправить без форматирования
            if "Can't parse entities" in str(e):
                logger.warning(f"Markdown formatting error: {e}, sending without formatting")
                kwargs.pop('parse_mode', None)
                return await original_reply_text(self, text, **kwargs)
            else:
                # Другие типы ошибок пробрасываем дальше
                raise
        except Exception as e:
            # Логируем любые другие ошибки
            logger.error(f"Message send error: {e}")
            raise
    
    # Заменяем оригинальный метод на безопасную версию
    Message.reply_text = patched_reply_text
    logger.info("✅ Telegram Message.reply_text patched for safe message sending")


async def safe_edit_message_text(query, text, reply_markup=None, parse_mode=None, **kwargs):
    """
    Безопасно редактирует сообщение инлайн-меню с полной обработкой всех типов ошибок:
    - игнорирует 'Message is not modified'
    - обрабатывает ошибки форматирования
    - при необходимости просто отвечает на клик, чтобы убрать «часики»
    - добавляет retry логику для временных сбоев
    """
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            return await query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                **kwargs
            )
            
        except BadRequest as e:
            error_msg = str(e)
            
            if "Message is not modified" in error_msg:
                # Содержимое не изменилось - это нормально, просто убираем "часики"
                try:
                    await query.answer(cache_time=1)
                    logger.debug("Message content unchanged, answered callback query")
                except Exception as answer_e:
                    logger.debug(f"Failed to answer callback query: {answer_e}")
                return None
                
            elif "Can't parse entities" in error_msg:
                # Проблема с форматированием - пробуем без parse_mode
                logger.warning(f"Parse error: {e}, retrying without formatting")
                try:
                    return await query.edit_message_text(
                        text=text,
                        reply_markup=reply_markup,
                        **kwargs  # без parse_mode
                    )
                except Exception as retry_e:
                    logger.warning(f"Retry without formatting failed: {retry_e}")
                    
            elif "Message to edit not found" in error_msg:
                # Сообщение уже удалено/недоступно
                logger.warning("Message to edit not found, skipping edit")
                try:
                    await query.answer("Сообщение недоступно", show_alert=False)
                except Exception:
                    pass
                return None
                
            elif "Bad Request: chat not found" in error_msg:
                # Чат недоступен
                logger.error("Chat not found, cannot edit message")
                return None
                
            else:
                # Другие BadRequest ошибки
                if attempt < max_retries - 1:
                    logger.warning(f"BadRequest on attempt {attempt + 1}: {e}, retrying...")
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    logger.error(f"Failed to edit message after {max_retries} attempts: {e}")
                    try:
                        await query.answer(f"Ошибка: {e}", show_alert=True)
                    except Exception:
                        pass
                    raise
                    
        except Exception as e:
            # Другие типы ошибок (сетевые и т.д.)
            if attempt < max_retries - 1:
                logger.warning(f"Network error on attempt {attempt + 1}: {e}, retrying...")
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            else:
                logger.error(f"Failed to edit message after {max_retries} attempts: {e}")
                logger.error(f"Full traceback: {traceback.format_exc()}")
                try:
                    await query.answer("Произошла ошибка", show_alert=True)
                except Exception:
                    pass
                raise


async def safe_reply_text(update, text, parse_mode=None, **kwargs):
    """
    Безопасно отправляет сообщение, обрабатывая ошибки форматирования.
    В случае ошибки отправляет сообщение без форматирования.
    """
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            # Сначала пробуем отправить сообщение как есть
            return await update.message.reply_text(text, parse_mode=parse_mode, **kwargs)
            
        except BadRequest as e:
            if "Can't parse entities" in str(e):
                logger.warning(f"Formatting error: {e}, sending without formatting")
                # В случае ошибки отправляем без форматирования
                try:
                    return await update.message.reply_text(text, parse_mode=None, **kwargs)
                except Exception as retry_e:
                    logger.error(f"Failed to send without formatting: {retry_e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                        continue
                    raise
            else:
                # Другие BadRequest ошибки
                if attempt < max_retries - 1:
                    logger.warning(f"BadRequest on attempt {attempt + 1}: {e}, retrying...")
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                raise
                
        except Exception as e:
            logger.error(f"Message send error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            else:
                # Пробуем отправить упрощенное сообщение об ошибке
                try:
                    return await update.message.reply_text(
                        f"❌ Ошибка отправки сообщения: {str(e)[:100]}..."
                    )
                except Exception:
                    # Если и это не удалось, просто пробрасываем ошибку
                    raise


async def safe_answer_callback_query(query, text=None, show_alert=False, **kwargs):
    """
    Безопасно отвечает на callback query с обработкой ошибок.
    """
    try:
        await query.answer(text=text, show_alert=show_alert, **kwargs)
    except BadRequest as e:
        if "Query is too old" in str(e):
            logger.debug("Callback query too old, ignoring")
        else:
            logger.warning(f"Failed to answer callback query: {e}")
    except Exception as e:
        logger.error(f"Unexpected error answering callback query: {e}")


def setup_safe_telegram_handlers():
    """
    Настраивает безопасные обработчики для всех Telegram операций.
    Вызывать один раз при инициализации бота.
    """
    patch_telegram_methods()
    logger.info("✅ Safe Telegram handlers configured")
