import aiohttp
import asyncio
import os
import logging
from telegram import Update, constants
from telegram.ext import Application, CommandHandler, ContextTypes
from collections import defaultdict
import re

# === CONFIG ===
TOKEN = "8443542210:AAESItt1B3EC-YxLVS0sqUqjInQDjRYw_cc"
API_URL = "https://learn.aakashitutor.com/get/test/syllabus?nid="
DEFAULT_BATCH_SIZE = 2000  # Increased from 500 to 2000
OWNER_ID = 7927314662  # Owner Telegram user ID

# === LOGGING ===
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === GLOBAL STATE ===
ongoing_searches = {}
checked_nid_counts = defaultdict(int)
total_nids_to_check = {}
valid_nids_found = defaultdict(int)
authorized_users = set()  # Set of authorized user IDs

# === Helper Functions ===

def is_authorized(user_id: int) -> bool:
    """Check if user is authorized to use the bot."""
    return user_id == OWNER_ID or user_id in authorized_users

def escape_markdown_v2(text: str) -> str:
    """Helper function to escape special characters for MarkdownV2."""
    special_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f"([{re.escape(special_chars)}])", r"\\\1", text)

async def fetch_test_data(session, nid):
    """
    Fetches test metadata for a given NID.
    Returns (nid, title) only if it's an actual TEST, not individual questions.
    """
    try:
        async with session.get(f"{API_URL}{nid}", timeout=10) as resp:  # Reduced from 15 to 10 seconds
            logger.debug(f"Checking NID {nid}: HTTP {resp.status}")
            if resp.status == 200:
                data = await resp.json()
                
                # Check if response has both 'title' AND 'quiz_desc' fields
                if isinstance(data, dict) and 'title' in data and 'quiz_desc' in data:
                    title = data.get("title", "")
                    quiz_desc = data.get("quiz_desc", "")
                    
                    # Skip if title or quiz_desc is empty
                    if not title or not quiz_desc or len(quiz_desc.strip()) == 0:
                        logger.debug(f"NID {nid}: Empty title or quiz_desc")
                        return nid, None
                    
                    title_lower = title.lower()
                    
                    # MODIFIED: Use word boundary matching for better accuracy
                    test_keywords = [
                        'jee', 'neet', 'test', 'aiats', 'studying', 'xi', 'xii'
                    ]
                    
                    # Check if any keyword exists as a whole word (not part of another word)
                    has_test_keyword = False
                    for keyword in test_keywords:
                        # Use word boundary regex to match whole words only
                        if re.search(rf'\b{keyword}\b', title_lower):
                            has_test_keyword = True
                            break
                    
                    if not has_test_keyword:
                        logger.debug(f"NID {nid}: Filtered out - no test keywords in title")
                        return nid, None
                    
                    # Skip if title ends with question mark (it's a question, not a test)
                    if title.endswith('?'):
                        logger.debug(f"NID {nid}: Filtered out - ends with question mark")
                        return nid, None
                    
                    # Skip if title ends with NID pattern (like "-4391186142")
                    if re.search(r'-\d{10}$', title):
                        logger.debug(f"NID {nid}: Filtered out - ends with NID pattern")
                        return nid, None
                    
                    # Filter out unwanted patterns
                    skip_patterns = [
                        'solution for question',
                        'video-',
                        'subscription',
                        '-subscription',
                        'question -',
                        'answer for',
                        'solution for',
                        '_utm_attributes',
                        '-registration',
                    ]
                    
                    for pattern in skip_patterns:
                        if pattern in title_lower:
                            logger.debug(f"NID {nid}: Filtered out - contains '{pattern}'")
                            return nid, None
                    
                    # Skip if title is mostly numbers or IDs
                    if re.match(r'^[\d\-]+$', title):
                        logger.debug(f"NID {nid}: Filtered out - only numbers/dashes")
                        return nid, None
                    
                    # Skip if title starts with numbers followed by dash
                    if re.match(r'^\d+-\d+', title):
                        logger.debug(f"NID {nid}: Filtered out - numeric ID pattern")
                        return nid, None
                    
                    # Skip if title is just invalid values
                    if title.lower() in ['none', 'null', '', 'n/a']:
                        logger.debug(f"NID {nid}: Filtered out - invalid title")
                        return nid, None
                    
                    # Skip if title is very short (real test names are usually longer)
                    if len(title.strip()) < 5:
                        logger.debug(f"NID {nid}: Filtered out - title too short")
                        return nid, None
                    
                    # Additional check: If title is very long and looks like a question (contains common question words)
                    question_indicators = ['which of the following', 'what is', 'how much', 'how many', 
                                         'calculate', 'find', 'determine', 'select the correct',
                                         'choose the correct', 'identify', 'consider']
                    has_question_indicator = any(indicator in title_lower for indicator in question_indicators)
                    
                    if has_question_indicator and len(title) > 50:
                        logger.debug(f"NID {nid}: Filtered out - appears to be a question based on content")
                        return nid, None
                    
                    # If we passed all filters, this is a valid test
                    logger.info(f"✅✅✅ VALID TEST NID: {nid} = {title}")
                    return nid, escape_markdown_v2(title)
                else:
                    logger.debug(f"NID {nid}: No title or quiz_desc in response")
            else:
                logger.debug(f"NID {nid}: HTTP {resp.status}")
    except aiohttp.ClientError as e:
        logger.warning(f"Network error fetching NID {nid}: {e}")
    except asyncio.TimeoutError:
        logger.warning(f"Timeout fetching NID {nid}")
    except Exception as e:
        logger.error(f"Unexpected error fetching NID {nid}: {e}")
    return nid, None

async def perform_search(chat_id: int, start_nid: int, end_nid: int, batch_size: int, context: ContextTypes.DEFAULT_TYPE):
    """
    Performs the NID search within the specified range.
    This function runs as a separate task.
    """
    message = None
    total_nids = end_nid - start_nid + 1
    total_nids_to_check[chat_id] = total_nids
    valid_nids_found[chat_id] = 0

    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=constants.ChatAction.TYPING)
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=rf"🔍 Starting NID search from `{start_nid}` to `{end_nid}`\.""\n"
                 rf"Total NIDs to check: `{total_nids}`\.""\n"
                 rf"Progress: `0` / `{total_nids}` \| Valid Tests Found: `0`",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )

        async with aiohttp.ClientSession() as session:
            for i in range(start_nid, end_nid + 1, batch_size):
                if chat_id not in ongoing_searches or ongoing_searches[chat_id].done():
                    logger.info(f"Search for chat {chat_id} cancelled or finished externally.")
                    await context.bot.send_message(
                        chat_id=chat_id, 
                        text=r"⏹️ Search cancelled\.", 
                        parse_mode=constants.ParseMode.MARKDOWN_V2
                    )
                    return

                batch_end = min(i + batch_size - 1, end_nid)
                batch = range(i, batch_end + 1)
                tasks = [fetch_test_data(session, nid) for nid in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        logger.debug(f"Skipping exception in batch: {result}")
                        continue

                    nid, title = result
                    checked_nid_counts[chat_id] += 1

                    if title:
                        valid_nids_found[chat_id] += 1
                        # Send immediately to avoid message length issues
                        logger.info(f"✅ FOUND VALID TEST NID: {nid} - Title: {title}")
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id, 
                                text=rf"✅ *Test Found\!*""\n"
                                     rf"📝 *Title:* {title}""\n"
                                     rf"🔢 *NID:* `{nid}`",
                                parse_mode=constants.ParseMode.MARKDOWN_V2
                            )
                        except Exception as e:
                            logger.error(f"Error sending message for NID {nid}: {e}")
                            # Fallback without bold formatting
                            await context.bot.send_message(
                                chat_id=chat_id, 
                                text=rf"✅ Found: {title} \(NID: `{nid}`\)",
                                parse_mode=constants.ParseMode.MARKDOWN_V2
                            )
                        await asyncio.sleep(0.05)  # Reduced from 0.1 to 0.05 seconds
                
                # Reduced delay between batches
                await asyncio.sleep(0.2)  # Reduced from 0.5 to 0.2 seconds

                # Update progress every 2000 NIDs instead of 1000
                if checked_nid_counts[chat_id] % 2000 == 0 or (batch_end == end_nid):
                    if message:
                        current_checked = checked_nid_counts[chat_id]
                        total_nids_val = total_nids_to_check.get(chat_id, total_nids)
                        found_count = valid_nids_found.get(chat_id, 0)
                        
                        try:
                            await message.edit_text(
                                rf"🔍 Searching NIDs from `{start_nid}` to `{end_nid}`\.""\n"
                                rf"Progress: `{current_checked}` / `{total_nids_val}` \| Valid Tests Found: `{found_count}`",
                                parse_mode=constants.ParseMode.MARKDOWN_V2
                            )
                        except Exception as e:
                            logger.warning(f"Could not update progress message: {e}")

        # Final summary
        final_checked = checked_nid_counts.get(chat_id, 0)
        final_found = valid_nids_found.get(chat_id, 0)
        await context.bot.send_message(
            chat_id=chat_id,
            text=rf"✅ *Search Complete\!*""\n"
                 rf"📊 Total NIDs Checked: `{final_checked}`""\n"
                 rf"✅ Valid Tests Found: `{final_found}`",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        logger.info(f"Search for chat {chat_id} from {start_nid} to {end_nid} completed. Found {final_found} valid tests.")

    except asyncio.CancelledError:
        await context.bot.send_message(
            chat_id=chat_id, 
            text=r"⏹️ Search gracefully cancelled\.", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        logger.info(f"Search task for chat {chat_id} was cancelled.")
    except Exception as e:
        logger.error(f"Error during search for chat {chat_id}: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=chat_id, 
            text=f"❌ An error occurred during the search: `{escape_markdown_v2(str(e))}`", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
    finally:
        if chat_id in ongoing_searches:
            del ongoing_searches[chat_id]
        if chat_id in checked_nid_counts:
            del checked_nid_counts[chat_id]
        if chat_id in total_nids_to_check:
            del total_nids_to_check[chat_id]
        if chat_id in valid_nids_found:
            del valid_nids_found[chat_id]

        if message:
            try:
                final_checked = checked_nid_counts.get(chat_id, 0)
                final_found = valid_nids_found.get(chat_id, 0)
                await message.edit_text(
                    rf"✅ Search session ended\.""\n"
                    rf"Total NIDs checked: `{final_checked}` \| Valid Tests: `{final_found}`",
                    parse_mode=constants.ParseMode.MARKDOWN_V2
                )
            except Exception as e:
                logger.warning(f"Could not edit final status message for chat {chat_id}: {e}")


# === Telegram Command Handlers ===

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a welcome message and explains available commands."""
    user_id = update.effective_user.id
    
    if not is_authorized(user_id):
        await update.message.reply_text(
            r"❌ You are not authorized to use this bot\.""\n"
            r"Please contact the bot owner for access\.",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        return
    
    await update.message.reply_text(
        r"👋 Welcome\! I can help you search for Test NIDs with syllabus data on Aakash iTutor\.""\n\n"
        r"🎯 *What I find:*""\n"
        r"• Actual Tests \(JEE, NEET, AIATS, Test Series, Mock Tests, etc\.\)""\n"
        r"• Filters out questions, solutions, videos, subscriptions""\n\n"
        r"📝 *Commands:*""\n"
        r"• `/search <start_nid> <end_nid>` \- Search for test NIDs\.""\n"
        r"  Example: `/search 843171378 843171500`""\n"
        r"• `/cancel` \- Stop ongoing search\.""\n"
        r"• `/status` \- Check search progress\.""\n"
        r"• `/help` \- Show this help message\.",
        parse_mode=constants.ParseMode.MARKDOWN_V2
    )

async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles the /search command.
    Usage: /search <start_nid> <end_nid> [batch_size]
    """
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    if not is_authorized(user_id):
        await update.message.reply_text(
            r"❌ You are not authorized to use this bot\.",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        return

    if chat_id in ongoing_searches and not ongoing_searches[chat_id].done():
        await update.message.reply_text(
            r"⏳ You already have an active search running\. Please `/cancel` it first if you want to start a new one\.", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            r"*Usage:* `/search <start_nid> <end_nid> [batch_size]`""\n\n"
            r"*Example:*""\n"
            r"`/search 843171378 843171500`""\n\n"
            r"The `batch_size` is optional and defaults to 500\.",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        return

    try:
        start_nid = int(args[0])
        end_nid = int(args[1])
        batch_size = DEFAULT_BATCH_SIZE
        
        if len(args) > 2:
            batch_size = int(args[2])
            if not (1 <= batch_size <= 10000):
                await update.message.reply_text(
                    r"Batch size must be between 1 and 10000\.", 
                    parse_mode=constants.ParseMode.MARKDOWN_V2
                )
                return

        # Allow 8-10 digit NIDs
        if not (10000000 <= start_nid <= 9999999999) or not (10000000 <= end_nid <= 9999999999):
            await update.message.reply_text(
                r"NID values must be between 8\-10 digits\.", 
                parse_mode=constants.ParseMode.MARKDOWN_V2
            )
            return

        if start_nid > end_nid:
            await update.message.reply_text(
                r"`start_nid` cannot be greater than `end_nid`\.", 
                parse_mode=constants.ParseMode.MARKDOWN_V2
            )
            return

        total_range_nids = end_nid - start_nid + 1
        if total_range_nids > 50000000:
            await update.message.reply_text(
                r"The requested NID range is too large\. Please specify a range of maximum 50,000,000 NIDs at a time\.",
                parse_mode=constants.ParseMode.MARKDOWN_V2
            )
            return

        checked_nid_counts[chat_id] = 0
        total_nids_to_check[chat_id] = total_range_nids
        valid_nids_found[chat_id] = 0
        
        task = asyncio.create_task(perform_search(chat_id, start_nid, end_nid, batch_size, context))
        ongoing_searches[chat_id] = task

    except ValueError:
        await update.message.reply_text(
            r"Please provide valid numerical NID values and an optional batch size\.", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Error in search_command for chat {chat_id}: {e}", exc_info=True)
        await update.message.reply_text(
            f"An unexpected error occurred: `{escape_markdown_v2(str(e))}`", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels an ongoing NID search for the current chat."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    if not is_authorized(user_id):
        await update.message.reply_text(
            r"❌ You are not authorized to use this bot\.",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        return

    if chat_id in ongoing_searches and not ongoing_searches[chat_id].done():
        ongoing_searches[chat_id].cancel()
        await update.message.reply_text(
            r"⏹️ Cancelling your ongoing NID search\. Please wait a moment\.\.\.", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
    else:
        await update.message.reply_text(
            r"You don't have an active NID search to cancel\.", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provides the status of the ongoing NID search."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    if not is_authorized(user_id):
        await update.message.reply_text(
            r"❌ You are not authorized to use this bot\.",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        return

    if chat_id in ongoing_searches and not ongoing_searches[chat_id].done():
        checked_count = checked_nid_counts.get(chat_id, 0)
        total_count = total_nids_to_check.get(chat_id, "N/A")
        found_count = valid_nids_found.get(chat_id, 0)
        
        await update.message.reply_text(
            rf"🔍 *Search Status*""\n"
            rf"Progress: `{checked_count}` / `{total_count}` NIDs""\n"
            rf"Valid Tests Found: `{found_count}`", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
    else:
        await update.message.reply_text(
            r"No active NID search found for your chat\.", 
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )

async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Authorize a user to use the bot (Owner only)."""
    user_id = update.effective_user.id
    
    if user_id != OWNER_ID:
        # Silently ignore if not owner - hidden command
        return
    
    args = context.args
    if len(args) < 1:
        await update.message.reply_text(
            r"*Usage:* `/auth <user_id>`""\n\n"
            r"*Example:* `/auth 123456789`",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        return
    
    try:
        target_user_id = int(args[0])
        authorized_users.add(target_user_id)
        await update.message.reply_text(
            rf"✅ User `{target_user_id}` has been authorized\.",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        logger.info(f"Owner {user_id} authorized user {target_user_id}")
    except ValueError:
        await update.message.reply_text(
            r"❌ Please provide a valid user ID\.",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )

async def unauth_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove authorization from a user (Owner only)."""
    user_id = update.effective_user.id
    
    if user_id != OWNER_ID:
        # Silently ignore if not owner - hidden command
        return
    
    args = context.args
    if len(args) < 1:
        await update.message.reply_text(
            r"*Usage:* `/unauth <user_id>`""\n\n"
            r"*Example:* `/unauth 123456789`",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
        return
    
    try:
        target_user_id = int(args[0])
        if target_user_id in authorized_users:
            authorized_users.remove(target_user_id)
            await update.message.reply_text(
                rf"✅ User `{target_user_id}` has been unauthorized\.",
                parse_mode=constants.ParseMode.MARKDOWN_V2
            )
            logger.info(f"Owner {user_id} unauthorized user {target_user_id}")
        else:
            await update.message.reply_text(
                rf"❌ User `{target_user_id}` was not authorized\.",
                parse_mode=constants.ParseMode.MARKDOWN_V2
            )
    except ValueError:
        await update.message.reply_text(
            r"❌ Please provide a valid user ID\.",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )

async def authlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all authorized users (Owner only)."""
    user_id = update.effective_user.id
    
    if user_id != OWNER_ID:
        # Silently ignore if not owner - hidden command
        return
    
    if not authorized_users:
        await update.message.reply_text(
            r"📋 *Authorized Users:* None""\n\n"
            rf"Owner ID: `{OWNER_ID}`",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )
    else:
        user_list = "\n".join([f"• `{uid}`" for uid in sorted(authorized_users)])
        await update.message.reply_text(
            rf"📋 *Authorized Users:*""\n"
            rf"{user_list}""\n\n"
            rf"Owner ID: `{OWNER_ID}`",
            parse_mode=constants.ParseMode.MARKDOWN_V2
        )

# === Main function ===
async def main():
    """Starts the bot."""
    application = Application.builder().token(TOKEN).updater(None).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", start_command))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("auth", auth_command))
    application.add_handler(CommandHandler("unauth", unauth_command))
    application.add_handler(CommandHandler("authlist", authlist_command))

    logger.info("🚀 Bot is starting...")
    
    await application.initialize()
    await application.start()
    
    offset = 0
    try:
        while True:
            try:
                updates = await application.bot.get_updates(
                    offset=offset,
                    timeout=30,
                    allowed_updates=Update.ALL_TYPES
                )
                
                for update in updates:
                    offset = update.update_id + 1
                    await application.process_update(update)
                    
            except Exception as e:
                logger.error(f"Error during polling: {e}")
                await asyncio.sleep(1)
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":

    asyncio.run(main())
