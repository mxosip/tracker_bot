import os
import json
import time
import hmac
import hashlib
import logging
import sys
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import requests
import calendar as cal

# Configure logging
logging.basicConfig(
    stream=sys.stdout,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment variables
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TRACKER_TOKEN = os.getenv('YANDEX_TRACKER_TOKEN')
CURRENT_USER = os.getenv('CURRENT_USER', 'XXXXXXX')

# Define the audience and region choices with emojis
AUDIENCE_CHOICES = [
    "👥 Users",        # Users group emoji
    "🚗 Drivers",      # Red car emoji
    "🏢 Partner park"  # Office building emoji
]

DONE_SELECTION = "✅ Done"

REGION_CHOICES = [
    "🌎 All regions",           # Americas-focused globe
    "🌍 South&Central Africa",  # Europe/Africa-focused globe
    "🌍 West Africa",           # Europe/Africa-focused globe
    "🌍 EMEA&Eur",             # Europe/Africa-focused globe
    "🌏 MENAP",                # Asia/Australia-focused globe
    "🌎 LatAm",                # Americas-focused globe
    "🌍 CIS",                  # Europe/Africa-focused globe
    "❓ I don't know"           # Question mark for unknown
]

# Common questions that appear after region selection (except All regions)
REGION_SPECIFIC_QUESTIONS = [
    "Which country?",
    "Which city?"
]

# Common questions for all paths
COMMON_QUESTIONS = [
    "What is the task about? (What has happened?)",
    "What problem do we want to solve with this communication?",
    "RTB",
    "Key message",
    "What would be your indicator that the problem was solved with the help of this communication?"
]

# Final questions for communication types
FINAL_QUESTIONS = [
    "Which segment of users/drivers should the communication be sent to?",
    "What types of communications you would like to use in this task?"
]

# Communication type choices
USER_COMMUNICATION_TYPES = [
    "📱 Push",
    "💬 SMS",
    "📲 WhatsApp",
    "🖼️ Banner",
    "📖 Stories",
    "📺 Fullscreen",
    "🎯 Plashka",
    "🗺️ Object over the map",
    "🔘 Promo button",
    "🎫 Promo card",
    "🎴 Upsell card",
    "✨ Splashscreen",
    "❓ I don't know"
]

DRIVER_COMMUNICATION_TYPES = [
    "📱 Push",
    "💬 SMS",
    "📲 WhatsApp",
    "📖 Stories",
    "📺 Fullscreen",
    "📰 Feed",
    "🗺️ Object over the map",
    "❓ I don't know"
]

# Navigation buttons
NAVIGATION_BUTTONS = [
    "⬅️ Go back",
    "❌ Cancel"
]

# Country flags
COUNTRY_FLAGS = {
    "Russia": "🇷🇺",
    "Kazakhstan": "🇰🇿",
    "Belarus": "🇧🇾",
    "Ukraine": "🇺🇦",
    "Germany": "🇩🇪",
    "France": "🇫🇷",
    "Spain": "🇪🇸",
    "Italy": "🇮🇹",
    "United Kingdom": "🇬🇧"
    # Add more countries as needed
}

# Store user states
user_states = {}

def get_current_time_utc():
    """Get current UTC time in YYYY-MM-DD HH:MM:SS format"""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def calculate_priority(deadline_str: str) -> str:
    """Calculate priority based on deadline"""
    deadline = datetime.strptime(deadline_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    days_until_deadline = (deadline - now).days
    
    if days_until_deadline < 3:
        return "blocker"
    elif days_until_deadline < 7:
        return "critical"    # Changed from "high" to "critical"
    elif days_until_deadline < 14:
        return "major"       # Changed from "normal" to "major"
    else:
        return "normal"      # Changed from "low" to "normal"

class YandexTracker:
    BASE_URL = "https://st-api.yandex-team.ru"
    DEFAULT_QUEUE = "YANGOCRM"
    METADATA_URL = "XXXXXXX"
    
    def __init__(self):
        self.current_user = os.getenv('CURRENT_USER', 'XXXXX')
        self.current_time = datetime.strptime("2025-06-09 14:49:11", "%Y-%m-%d %H:%M:%S")
        logger.info(f"Initialized YandexTracker client for user: {self.current_user}")

    def _get_iam_token(self):
        try:
            logger.info("Attempting to get IAM token from metadata service...")
            headers = {
                "Metadata-Flavor": "Google"  # Required header for metadata service
            }
            
            response = requests.get(
                self.METADATA_URL,
                headers=headers,
                timeout=3.05
            )
            
            if response.ok:
                token_data = response.json()
                return token_data.get('access_token')
            else:
                raise Exception(f"Metadata service returned status {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error getting IAM token: {e}")
            raise

    def create_issue(self, queue, summary, description, priority="normal", assignee=None):
        try:
            iam_token = self._get_iam_token()
            
            headers = {
                "Authorization": f"Bearer {iam_token}",
                "Content-Type": "application/json",
                "X-Ya-User-Login": self.current_user
            }

            data = {
                "queue": queue or self.DEFAULT_QUEUE,
                "summary": summary,
                "description": description,
                "type": {"name": "Task"},
                "priority": priority,
                "createdBy": self.current_user,
                "createdAt": self.current_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            }
            
            if assignee:
                data["assignee"] = assignee

            endpoint = f"{self.BASE_URL}/v2/issues/"
            
            logger.info(f"Creating issue at {self.current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info(f"Queue: {queue or self.DEFAULT_QUEUE}")
            logger.info(f"User: {self.current_user}")
            logger.info(f"Request data: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            response = requests.post(
                endpoint,
                headers=headers,
                json=data,
                timeout=(3.05, 27)
            )
            
            logger.info(f"Response status code: {response.status_code}")
            
            if not response.ok:
                try:
                    error_data = response.json()
                    logger.error(f"Error response data: {json.dumps(error_data, ensure_ascii=False, indent=2)}")
                    error_message = self._format_error_message(error_data)
                except:
                    error_message = response.text
                    logger.error(f"Raw error response: {response.text}")
                
                raise Exception(f"API Error ({response.status_code}): {error_message}")
            
            response_data = response.json()
            logger.info(f"Successfully created issue: {json.dumps(response_data, ensure_ascii=False, indent=2)}")
            return response_data
            
        except Exception as e:
            logger.error(f"Error creating issue: {str(e)}")
            raise

    def _format_error_message(self, error_data):
        if isinstance(error_data, dict):
            if 'errors' in error_data:
                return '; '.join(f"{k}: {v}" for k, v in error_data['errors'].items())
            elif 'errorMessages' in error_data:
                return '; '.join(error_data['errorMessages'])
            elif 'message' in error_data:
                return error_data['message']
        return str(error_data)

class FormState:
    def __init__(self):
        self.answers: Dict[str, Any] = {}
        self.current_question: str = None
        self.selected_audience: bool = False
        self.selected_region: str = None
        self.questions_queue: List[str] = []
        self.awaiting_deadline: bool = False
        self.current_calendar_year: int = datetime.now().year
        self.current_calendar_month: int = datetime.now().month
        self.previous_states: List[Dict] = []
        self.awaiting_communication_types: bool = False
        self.communication_types: List[str] = []
        self.all_questions_answered: bool = False
        self.is_empty_ticket: bool = False  # New flag for empty ticket flow

    def save_state(self):
        """Save current state for going back"""
        state_copy = {
            'answers': self.answers.copy(),
            'current_question': self.current_question,
            'selected_audience': self.selected_audience,
            'selected_region': self.selected_region,
            'questions_queue': self.questions_queue.copy() if self.questions_queue else [],
            'awaiting_deadline': self.awaiting_deadline,
            'awaiting_communication_types': self.awaiting_communication_types,
            'communication_types': self.communication_types.copy() if self.communication_types else [],
            'all_questions_answered': self.all_questions_answered  # Save the new flag
        }
        self.previous_states.append(state_copy)

    def go_back(self) -> bool:
        """Restore previous state. Returns True if successful, False if no previous state"""
        if not self.previous_states:
            return False
        
        previous_state = self.previous_states.pop()
        self.answers = previous_state['answers']
        self.current_question = previous_state['current_question']
        self.selected_audience = previous_state['selected_audience']
        self.selected_region = previous_state['selected_region']
        self.questions_queue = previous_state['questions_queue']
        self.awaiting_deadline = previous_state['awaiting_deadline']
        self.awaiting_communication_types = previous_state['awaiting_communication_types']
        self.communication_types = previous_state['communication_types']
        self.all_questions_answered = previous_state.get('all_questions_answered', False)  # Restore the new flag
        return True

    def get_next_question(self) -> str:
        if not self.questions_queue:
            self.all_questions_answered = True
            return None
        self.current_question = self.questions_queue.pop(0)
        return self.current_question

def get_keyboard_markup(choices: List[str]) -> ReplyKeyboardMarkup:
    keyboard = [[choice] for choice in choices]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def create_calendar_keyboard(year: int, month: int) -> InlineKeyboardMarkup:
    """Create an inline keyboard with a calendar"""
    keyboard = []
    
    # Add month and year at the top
    month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
                  'August', 'September', 'October', 'November', 'December']
    keyboard.append([InlineKeyboardButton(f"{month_names[month-1]} {year}",
                                        callback_data="ignore")])
    
    # Add days of week as header
    days_of_week = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su']
    keyboard.append([InlineKeyboardButton(day, callback_data="ignore") for day in days_of_week])
    
    # Get the calendar for current month
    month_calendar = cal.monthcalendar(year, month)
    
    # Add calendar days
    today = datetime.now(timezone.utc).date()
    for week in month_calendar:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            else:
                date = datetime(year, month, day).date()
                if date < today:
                    # Past dates are disabled
                    row.append(InlineKeyboardButton("✖", callback_data="ignore"))
                else:
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    row.append(InlineKeyboardButton(str(day), callback_data=f"date_{date_str}"))
        keyboard.append(row)
    
    # Add navigation buttons at the bottom
    nav_row = []
    prev_month = month - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    
    next_month = month + 1
    next_year = year
    if next_month == 13:
        next_month = 1
        next_year += 1
    
    nav_row.append(InlineKeyboardButton("<<", callback_data=f"month_{prev_year}_{prev_month}"))
    nav_row.append(InlineKeyboardButton(">>", callback_data=f"month_{next_year}_{next_month}"))
    keyboard.append(nav_row)
    
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    keyboard = [
        ['📝 Create Task', '📄 Empty ticket']  # Added new button
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        "Hello! I can help you create tasks in Yandex Tracker for Yango CRM Team.\nNOTE!This bot for only fast creation of tasks without access to tracker from your laptop. For big detailed projects please use our [Yandex Tracker queue](https://st.yandex-team.ru/createTicket?queue=YANGOCRM)\nChoose an action:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    user_states[update.effective_user.id] = {}

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel current operation"""
    user_id = update.effective_user.id
    user_states[user_id] = {}
    keyboard = [['📝 Create Task']]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        "Operation cancelled. What would you like to do?",
        reply_markup=reply_markup
    )
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel current operation"""
    user_id = update.effective_user.id
    user_states[user_id] = {}
    keyboard = [['📝 Create Task']]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        "Operation cancelled. What would you like to do?",
        reply_markup=reply_markup
    )

async def handle_back(update: Update, context: ContextTypes.DEFAULT_TYPE):  # Move this to the same level as other async functions
    """Handle going back to previous state"""
    user_id = update.effective_user.id
    if user_id not in user_states or 'form_state' not in user_states[user_id]:
        await start(update, context)
        return True

    form_state = user_states[user_id]['form_state']
    if form_state.go_back():
        # Determine appropriate message and keyboard based on current state
        if not form_state.selected_audience:
            await update.message.reply_text(
                "*For what audience is the communication planned?*\nSelect one or more options:",
                parse_mode='Markdown',
                reply_markup=get_keyboard_markup(AUDIENCE_CHOICES + [DONE_SELECTION] + NAVIGATION_BUTTONS)
            )
        elif not form_state.selected_region:
            await update.message.reply_text(
                "*Select regions:*\nSelect one option:",
                parse_mode='Markdown',
                reply_markup=get_keyboard_markup(REGION_CHOICES + NAVIGATION_BUTTONS)
            )
        elif form_state.awaiting_deadline:
            await update.message.reply_text(
                "*Select deadline:*",
                parse_mode='Markdown',
                reply_markup=create_calendar_keyboard(form_state.current_calendar_year, form_state.current_calendar_month)
            )
        elif form_state.awaiting_communication_types:
            comm_types = USER_COMMUNICATION_TYPES if "Users" in form_state.answers.get('audience', []) else DRIVER_COMMUNICATION_TYPES
            await update.message.reply_text(
                "*Select communication types:*\nYou can select multiple options:",
                parse_mode='Markdown',
                reply_markup=get_keyboard_markup(comm_types + [DONE_SELECTION] + NAVIGATION_BUTTONS)
            )
        else:
            await update.message.reply_text(
                f"*{form_state.current_question}*",
                parse_mode='Markdown',
                reply_markup=get_keyboard_markup(NAVIGATION_BUTTONS)
            )
        return True
    return False

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle calendar button clicks"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if user_id not in user_states or 'form_state' not in user_states[user_id]:
        await query.answer()
        return
    
    form_state = user_states[user_id]['form_state']
    
    if not form_state.awaiting_deadline:
        await query.answer()
        return

    try:
        await query.answer()  # Answer callback query to remove loading state
        
        if query.data == "ignore":
            return
        
        if query.data.startswith("date_"):
            form_state.save_state()  # Save state before making changes
            selected_date = query.data[5:]  # Remove "date_" prefix
            
            # Validate that selected date is not in the past
            selected_datetime = datetime.strptime(selected_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            if selected_datetime.date() < datetime.now(timezone.utc).date():
                await query.message.reply_text(
                    "❌ Cannot select a date in the past. Please choose a future date.",
                    reply_markup=create_calendar_keyboard(form_state.current_calendar_year, form_state.current_calendar_month)
                )
                return
            
            form_state.answers['deadline'] = selected_date
            form_state.awaiting_deadline = False
            
            # Calculate and store priority
            priority = calculate_priority(selected_date)
            form_state.answers['priority'] = priority
            
            # Delete the calendar message
            await query.message.delete()
            
            # Send confirmation of selected date and priority
            await query.message.reply_text(
                f"Selected deadline: {selected_date}\nPriority set to: {priority}"
            )
            
            # Create the task after deadline is set
            try:
                tracker = YandexTracker()
                description_parts = [
                    f"Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): {get_current_time_utc()}",
                    f"Current User's Login: {CURRENT_USER}",
                    f"\n⏰ Deadline: {form_state.answers['deadline']}",
                    f"⚡ Priority: {form_state.answers['priority'].upper()}\n"
                ]
                
                # Add audience with emojis
                description_parts.append("*For what audience is the communication planned?*")
                audience_with_emojis = []
                for aud in form_state.answers['audience']:
                    for choice in AUDIENCE_CHOICES:
                        if aud in choice:
                            audience_with_emojis.append(f"```{choice}```")
                            break
                description_parts.append("\n".join(audience_with_emojis))
                
                # Add region with emoji
                description_parts.append("\n*Selected region:*")
                for choice in REGION_CHOICES:
                    if form_state.answers['region'] in choice:
                        description_parts.append(f"```{choice}```")
                        break
                
                # Add country with flag if provided
                if "Which country?" in form_state.answers:
                    country = form_state.answers["Which country?"]
                    flag = COUNTRY_FLAGS.get(country, "")
                    description_parts.append(f"\n*Country:* {flag}{country}")
                
                # Add city if provided
                if "Which city?" in form_state.answers:
                    description_parts.append(f"\n*City:* {form_state.answers['Which city?']}")
                
                # Add all other Q&A
                for question in COMMON_QUESTIONS + FINAL_QUESTIONS:
                    if question in form_state.answers:
                        description_parts.append(f"\n*{question}*")
                        if question == "What types of communications you would like to use in this task?":
                            comm_types = form_state.answers[question].split(", ")
                            description_parts.append("\n".join([f"```{ct}```" for ct in comm_types]))
                        else:
                            description_parts.append(form_state.answers[question])
                
                full_description = "\n".join(description_parts)
                
                # Create issue
                issue = tracker.create_issue(
                    queue='YANGOCRM',
                    summary=form_state.answers["What is the task about? (What has happened?)"][:100],
                    description=full_description,
                    priority=form_state.answers['priority']
                )
                
                if issue:
                    await query.message.reply_text(
                        f"✅ Task created successfully!\n"
                        f"Key: {issue['key']}\n"
                        f"Link: https://st.yandex-team.ru/{issue['key']}",
                        reply_markup=get_keyboard_markup(['📝 Create Task'])
                    )
                else:
                    await query.message.reply_text(
                        "❌ Failed to create task.\n"
                        "Please contact support or try again later.",
                        reply_markup=get_keyboard_markup(['📝 Create Task'])
                    )
                
                # Clear user state
                user_states[user_id] = {}
                
            except Exception as e:
                logger.error(f"Error creating task: {str(e)}")
                await query.message.reply_text(
                    f"❌ Error creating task: {str(e)}",
                    reply_markup=get_keyboard_markup(['📝 Create Task'])
                )
                user_states[user_id] = {}
            
        elif query.data.startswith("month_"):
            # Month navigation
            _, year, month = query.data.split("_")
            form_state.current_calendar_year = int(year)
            form_state.current_calendar_month = int(month)
            await query.message.edit_reply_markup(
                reply_markup=create_calendar_keyboard(form_state.current_calendar_year, form_state.current_calendar_month)
            )
            
    except Exception as e:
        logger.error(f"Error in callback handler: {str(e)}")
        await query.message.reply_text(
            "❌ An error occurred while processing your selection. Please try again.",
            reply_markup=create_calendar_keyboard(form_state.current_calendar_year, form_state.current_calendar_month)
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message_text = update.message.text
    
    # Handle navigation buttons
    if message_text == "❌ Cancel":
        await cancel(update, context)
        return
    elif message_text == "⬅️ Go back":
        if await handle_back(update, context):
            return
    
    if user_id not in user_states:
        user_states[user_id] = {}
    
    # Handle Empty ticket creation
    if message_text == "📄 Empty ticket":
        form_state = FormState()
        user_states[user_id] = {
            'form_state': form_state,
            'state': 'creating_empty_ticket',
            'step': 'name'  # Start with name input
        }
        await update.message.reply_text(
            "Please enter the name of the task:",
            reply_markup=get_keyboard_markup(NAVIGATION_BUTTONS)
        )
        return
    
    # Handle empty ticket creation steps
    if user_id in user_states and user_states[user_id].get('state') == 'creating_empty_ticket':
        if user_states[user_id]['step'] == 'name':
            user_states[user_id]['task_name'] = message_text
            user_states[user_id]['step'] = 'description'
            await update.message.reply_text(
                "Please enter the description of the task:",
                reply_markup=get_keyboard_markup(NAVIGATION_BUTTONS)
            )
            return
        elif user_states[user_id]['step'] == 'description':
            try:
                # Create issue with minimal information
                tracker = YandexTracker()
                description_parts = [
                    f"Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): {get_current_time_utc()}",
                    f"Current User's Login: {CURRENT_USER}\n",
                    message_text  # Add the description
                ]
                
                issue = tracker.create_issue(
                    queue='YANGOCRM',
                    summary=user_states[user_id]['task_name'][:100],
                    description="\n".join(description_parts),
                    priority="normal"  # Default priority for empty tickets
                )
                
                if issue:
                    await update.message.reply_text(
                        f"✅ Empty task created successfully!\n"
                        f"Key: {issue['key']}\n"
                        f"Link: https://st.yandex-team.ru/{issue['key']}",
                        reply_markup=get_keyboard_markup(['📝 Create Task', '📄 Empty ticket'])
                    )
                else:
                    await update.message.reply_text(
                        "❌ Failed to create task.\n"
                        "Please contact support or try again later.",
                        reply_markup=get_keyboard_markup(['📝 Create Task', '📄 Empty ticket'])
                    )
                
                # Clear user state
                user_states[user_id] = {}
                
            except Exception as e:
                logger.error(f"Error creating empty task: {str(e)}")
                await update.message.reply_text(
                    f"❌ Error creating task: {str(e)}",
                    reply_markup=get_keyboard_markup(['📝 Create Task', '📄 Empty ticket'])
                )
                user_states[user_id] = {}
            return
    
    # Existing code for regular task creation
    if message_text == "📝 Create Task":
        # Initialize new form state
        form_state = FormState()
        user_states[user_id] = {
            'form_state': form_state,
            'state': 'collecting_data'
        }
        # Start with audience selection
        await update.message.reply_text(
            "*For what audience is the communication planned?*\nSelect one or more options:",
            parse_mode='Markdown',
            reply_markup=get_keyboard_markup(AUDIENCE_CHOICES + [DONE_SELECTION] + NAVIGATION_BUTTONS)
        )
        return

    if user_id not in user_states or 'form_state' not in user_states[user_id]:
        await start(update, context)
        return

    form_state: FormState = user_states[user_id]['form_state']
    state = user_states[user_id]['state']

    if state == 'collecting_data':
        if not form_state.selected_audience and 'audience' not in form_state.answers:
            # Initial audience selection
            if message_text in AUDIENCE_CHOICES:
                form_state.save_state()
                clean_text = ' '.join(message_text.split()[1:])
                form_state.answers['audience'] = [clean_text]
                await update.message.reply_text(
                    f"Selected: {message_text}\nYou can select more or click '{DONE_SELECTION}'",
                    reply_markup=get_keyboard_markup(AUDIENCE_CHOICES + [DONE_SELECTION] + NAVIGATION_BUTTONS)
                )
                return
        elif not form_state.selected_audience:
            # Additional audience selection
            if message_text == DONE_SELECTION:
                if not form_state.answers.get('audience'):
                    await update.message.reply_text(
                        "Please select at least one audience option.",
                        reply_markup=get_keyboard_markup(AUDIENCE_CHOICES + [DONE_SELECTION] + NAVIGATION_BUTTONS)
                    )
                    return
                form_state.save_state()
                form_state.selected_audience = True
                await update.message.reply_text(
                    "*Select regions:*\nSelect one option:",
                    parse_mode='Markdown',
                    reply_markup=get_keyboard_markup(REGION_CHOICES + NAVIGATION_BUTTONS)
                )
                return
            elif message_text in AUDIENCE_CHOICES:
                clean_text = ' '.join(message_text.split()[1:])
                if clean_text not in form_state.answers['audience']:
                    form_state.save_state()
                    form_state.answers['audience'].append(clean_text)
                await update.message.reply_text(
                    f"Selected: {message_text}\nYou can select more or click '{DONE_SELECTION}'",
                    reply_markup=get_keyboard_markup(AUDIENCE_CHOICES + [DONE_SELECTION] + NAVIGATION_BUTTONS)
                )
                return

        elif not form_state.selected_region:
            # Handling region selection
            if message_text in REGION_CHOICES:
                form_state.save_state()
                clean_text = ' '.join(message_text.split()[1:])
                form_state.selected_region = clean_text
                form_state.answers['region'] = clean_text
                
                # Set up questions queue right after region selection
                if clean_text == "All regions":
                    form_state.questions_queue = COMMON_QUESTIONS.copy() + FINAL_QUESTIONS.copy()
                else:
                    form_state.questions_queue = REGION_SPECIFIC_QUESTIONS.copy() + COMMON_QUESTIONS.copy() + FINAL_QUESTIONS.copy()
                
                # Start asking questions
                next_question = form_state.get_next_question()
                await update.message.reply_text(
                    f"*{next_question}*",
                    parse_mode='Markdown',
                    reply_markup=get_keyboard_markup(NAVIGATION_BUTTONS)
                )
                return
            else:
                await update.message.reply_text(
                    "Please select a region from the list.",
                    reply_markup=get_keyboard_markup(REGION_CHOICES + NAVIGATION_BUTTONS)
                )
                return

        elif form_state.current_question == FINAL_QUESTIONS[1]:  # Communication types question
            if not form_state.awaiting_communication_types:
                form_state.save_state()
                form_state.awaiting_communication_types = True
                comm_types = USER_COMMUNICATION_TYPES if "Users" in form_state.answers.get('audience', []) else DRIVER_COMMUNICATION_TYPES
                await update.message.reply_text(
                    "*Select communication types:*\nYou can select multiple options:",
                    parse_mode='Markdown',
                    reply_markup=get_keyboard_markup(comm_types + [DONE_SELECTION] + NAVIGATION_BUTTONS)
                )
                return
            elif message_text == DONE_SELECTION:
                if not form_state.communication_types:
                    await update.message.reply_text(
                        "Please select at least one communication type.",
                        reply_markup=get_keyboard_markup(
                            (USER_COMMUNICATION_TYPES if "Users" in form_state.answers.get('audience', []) else DRIVER_COMMUNICATION_TYPES) +
                            [DONE_SELECTION] + NAVIGATION_BUTTONS
                        )
                    )
                    return
                form_state.save_state()
                form_state.answers[form_state.current_question] = ", ".join(form_state.communication_types)
                form_state.awaiting_communication_types = False
                
                # After all questions are answered, ask for deadline
                form_state.awaiting_deadline = True
                await update.message.reply_text(
                    "*Select deadline:*",
                    parse_mode='Markdown',
                    reply_markup=create_calendar_keyboard(form_state.current_calendar_year, form_state.current_calendar_month)
                )
                return
                
            elif message_text in (USER_COMMUNICATION_TYPES if "Users" in form_state.answers.get('audience', []) else DRIVER_COMMUNICATION_TYPES):
                form_state.save_state()
                clean_text = message_text  # Keep emoji for communication types
                if clean_text not in form_state.communication_types:
                    form_state.communication_types.append(clean_text)
                await update.message.reply_text(
                    f"Selected: {message_text}\nYou can select more or click '{DONE_SELECTION}'",
                    reply_markup=get_keyboard_markup(
                        (USER_COMMUNICATION_TYPES if "Users" in form_state.answers.get('audience', []) else DRIVER_COMMUNICATION_TYPES) +
                        [DONE_SELECTION] + NAVIGATION_BUTTONS
                    )
                )
                return

        else:
            # Handling questions
            if form_state.current_question:
                form_state.save_state()
                form_state.answers[form_state.current_question] = message_text
            
            next_question = form_state.get_next_question()
            if next_question:
                if next_question == FINAL_QUESTIONS[1]:  # Communication types question
                    form_state.awaiting_communication_types = True
                    comm_types = USER_COMMUNICATION_TYPES if "Users" in form_state.answers.get('audience', []) else DRIVER_COMMUNICATION_TYPES
                    await update.message.reply_text(
                        "*Select communication types:*\nYou can select multiple options:",
                        parse_mode='Markdown',
                        reply_markup=get_keyboard_markup(comm_types + [DONE_SELECTION] + NAVIGATION_BUTTONS)
                    )
                else:
                    await update.message.reply_text(
                        f"*{next_question}*",
                        parse_mode='Markdown',
                        reply_markup=get_keyboard_markup(NAVIGATION_BUTTONS)
                    )
                return

async def setup_application():
    """Initialize and return the Application instance"""
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    await application.initialize()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    return application

async def process_telegram_update(update_dict: dict):
    """Process Telegram update asynchronously"""
    app = await setup_application()
    update = Update.de_json(update_dict, app.bot)
    await app.process_update(update)
    await app.shutdown()

def handler(event, context):
    """Cloud Functions handler"""
    current_time = get_current_time_utc()
    logger.info(f"Handler started at {current_time}")
    
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Content-Type': 'application/json'
    }
    
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 204,
            'headers': headers,
            'body': ''
        }
    
    try:
        if "body" in event:
            update_dict = json.loads(event["body"])
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(process_telegram_update(update_dict))
            loop.close()
            
            return {
                'statusCode': 200,
                'headers': headers,
                'body': 'ok'
            }
        else:
            return {
                'statusCode': 400,
                'headers': headers,
                'body': 'No body'
            }
            
    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': str(e)
        }
