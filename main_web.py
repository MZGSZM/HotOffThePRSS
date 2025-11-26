# main_web.py

import os
import json
import uuid
import yaml
import sys
import importlib.util
from datetime import datetime, timezone
from flask import Flask, render_template_string, request, redirect, url_for, flash, get_flashed_messages, send_file, session, g
from werkzeug.security import generate_password_hash, check_password_hash

# --- Get the absolute path of the script's directory ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Flask Web App Setup ---
app = Flask(__name__)

# --- Configuration & State Files (now with absolute paths) ---
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.json")
SENT_ARTICLES_FILE = os.path.join(SCRIPT_DIR, "sent_articles.yaml")
FEED_STATE_FILE = os.path.join(SCRIPT_DIR, "feed_state.json")
USER_FILE = os.path.join(SCRIPT_DIR, "user.json") # Stores the admin user(s) credentials
SECRET_KEY_FILE = os.path.join(SCRIPT_DIR, "secret.key") # Stores the Flask secret key
SCHEDULER_FILE = os.path.join(SCRIPT_DIR, "scheduler.py") # Path to scheduler script

# --- Set a common User-Agent for all feedparser requests ---
import feedparser
feedparser.USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0"

# --- Helper function for human-readable time ---
def time_ago(dt_str):
    if not dt_str:
        return "never"
    try:
        dt = datetime.fromisoformat(dt_str)
        now = datetime.now(timezone.utc)
        diff = now - dt
        
        seconds = diff.total_seconds()
        if seconds < 60:
            return "just now"
        elif seconds < 3600:
            return f"{int(seconds / 60)}m ago"
        elif seconds < 86400:
            return f"{int(seconds / 3600)}h ago"
        else:
            return f"{int(seconds / 86400)}d ago"
    except (ValueError, TypeError):
        return "invalid date"

# Make the helper function available in templates
app.jinja_env.globals.update(time_ago=time_ago)

# --- Helper to Import Scheduler Logic ---
def get_scheduler_check_function():
    """Dynamically imports the check_single_feed function from scheduler.py"""
    try:
        spec = importlib.util.spec_from_file_location("scheduler", SCHEDULER_FILE)
        scheduler_module = importlib.util.module_from_spec(spec)
        sys.modules["scheduler"] = scheduler_module
        spec.loader.exec_module(scheduler_module)
        return scheduler_module.check_single_feed, scheduler_module.load_feed_state, scheduler_module.save_feed_state
    except Exception as e:
        print(f"Error importing scheduler: {e}")
        return None, None, None

# --- HTML Templates ---

LAYOUT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en" class="h-full">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hot Off The PRSS</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>📡</text></svg>">
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: { sans: ['Inter', 'sans-serif'] },
                    colors: {
                        gray: { 900: '#0f172a', 800: '#1e293b', 700: '#334155' }
                    }
                }
            }
        }
    </script>
    <style>
        body { font-family: 'Inter', sans-serif; }
        /* Custom Scrollbar */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #475569; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #64748b; }
    </style>
    <script>
        // Dark mode logic
        if (localStorage.theme === 'dark' || (!('theme' in localStorage) && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
            document.documentElement.classList.add('dark')
        } else {
            document.documentElement.classList.remove('dark')
        }

        function toggleTheme() {
            if (document.documentElement.classList.contains('dark')) {
                document.documentElement.classList.remove('dark');
                localStorage.theme = 'light';
            } else {
                document.documentElement.classList.add('dark');
                localStorage.theme = 'dark';
            }
        }
    </script>
</head>
<body class="h-full bg-gray-50 text-gray-900 dark:bg-gray-900 dark:text-gray-100 transition-colors duration-200 flex flex-col">
    
    <!-- Navbar -->
    <nav class="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 sticky top-0 z-50 backdrop-blur-md bg-opacity-80 dark:bg-opacity-80">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="flex justify-between h-16">
                <div class="flex">
                    <div class="flex-shrink-0 flex items-center space-x-3">
                        <div class="bg-indigo-600 text-white p-2 rounded-lg">
                            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 5c7.18 0 13 5.82 13 13M6 11a7 7 0 017 7m-6 0a1 1 0 11-2 0 1 1 0 012 0z" /></svg>
                        </div>
                        <span class="font-bold text-xl tracking-tight hidden sm:block">Hot Off The PRSS</span>
                    </div>
                    {% if g.user %}
                    <div class="hidden sm:ml-8 sm:flex sm:space-x-8">
                        <a href="{{ url_for('view_feeds') }}" class="border-transparent text-gray-500 dark:text-gray-300 hover:border-indigo-500 hover:text-gray-700 dark:hover:text-white inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors">Dashboard</a>
                        <a href="{{ url_for('add_feed') }}" class="border-transparent text-gray-500 dark:text-gray-300 hover:border-indigo-500 hover:text-gray-700 dark:hover:text-white inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors">Add Feed</a>
                        <a href="{{ url_for('settings') }}" class="border-transparent text-gray-500 dark:text-gray-300 hover:border-indigo-500 hover:text-gray-700 dark:hover:text-white inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors">Settings</a>
                        <a href="{{ url_for('backup_restore') }}" class="border-transparent text-gray-500 dark:text-gray-300 hover:border-indigo-500 hover:text-gray-700 dark:hover:text-white inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors">Backup</a>
                    </div>
                    {% endif %}
                </div>
                <div class="flex items-center space-x-4">
                    <!-- Theme Toggle -->
                    <button onclick="toggleTheme()" class="text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 rounded-lg p-2.5 text-sm">
                        <svg id="theme-toggle-dark-icon" class="hidden dark:block w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path d="M17.293 13.293A8 8 0 016.707 2.707a8.001 8.001 0 1010.586 10.586z"></path></svg>
                        <svg id="theme-toggle-light-icon" class="block dark:hidden w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path d="M10 2a1 1 0 011 1v1a1 1 0 11-2 0V3a1 1 0 011-1zm4 8a4 4 0 11-8 0 4 4 0 018 0zm-.464 4.95l.707.707a1 1 0 001.414-1.414l-.707-.707a1 1 0 00-1.414 1.414zm2.12-10.607a1 1 0 010 1.414l-.706.707a1 1 0 11-1.414-1.414l.707-.707a1 1 0 011.414 0zM17 11a1 1 0 100-2h-1a1 1 0 100 2h1zm-7 4a1 1 0 011 1v1a1 1 0 11-2 0v-1a1 1 0 011-1zM5.05 6.464A1 1 0 106.465 5.05l-.708-.707a1 1 0 00-1.414 1.414l.707.707zm1.414 8.486l-.707.707a1 1 0 01-1.414-1.414l.707-.707a1 1 0 011.414 1.414zM4 11a1 1 0 100-2H3a1 1 0 000 2h1z" fill-rule="evenodd" clip-rule="evenodd"></path></svg>
                    </button>
                    
                    {% if g.user %}
                    <div class="flex items-center space-x-3">
                        <div class="hidden sm:flex flex-col items-end">
                            <span class="text-sm font-medium text-gray-700 dark:text-gray-200">{{ g.user.username }}</span>
                            <span class="text-xs text-gray-500 dark:text-gray-400 uppercase">{{ g.user.role|replace('_', ' ') }}</span>
                        </div>
                        <a href="{{ url_for('logout') }}" class="text-sm text-red-500 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300 font-medium">Log out</a>
                    </div>
                    {% endif %}
                </div>
            </div>
        </div>
        <!-- Mobile Menu (Simple) -->
        {% if g.user %}
        <div class="sm:hidden border-t border-gray-200 dark:border-gray-700">
            <div class="flex justify-around p-2">
                <a href="{{ url_for('view_feeds') }}" class="text-gray-600 dark:text-gray-300 p-2 text-sm font-medium">Dashboard</a>
                <a href="{{ url_for('add_feed') }}" class="text-gray-600 dark:text-gray-300 p-2 text-sm font-medium">Add Feed</a>
                <a href="{{ url_for('settings') }}" class="text-gray-600 dark:text-gray-300 p-2 text-sm font-medium">Settings</a>
            </div>
        </div>
        {% endif %}
    </nav>

    <div class="flex-grow py-8 px-4 sm:px-6 lg:px-8 max-w-7xl mx-auto w-full">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                <div class="mb-6 space-y-2">
                {% for category, message in messages %}
                    {% if category == 'success' %}
                    <div class="flex items-center p-4 mb-4 text-sm text-green-800 rounded-lg bg-green-50 dark:bg-gray-800 dark:text-green-400 border border-green-300 dark:border-green-800" role="alert">
                        <svg class="flex-shrink-0 inline w-4 h-4 mr-3" aria-hidden="true" xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 20 20"><path d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z"/></svg>
                        <span class="font-medium">{{ message }}</span>
                    </div>
                    {% else %}
                    <div class="flex items-center p-4 mb-4 text-sm text-red-800 rounded-lg bg-red-50 dark:bg-gray-800 dark:text-red-400 border border-red-300 dark:border-red-800" role="alert">
                        <svg class="flex-shrink-0 inline w-4 h-4 mr-3" aria-hidden="true" xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 20 20"><path d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z"/></svg>
                        <span class="font-medium">{{ message }}</span>
                    </div>
                    {% endif %}
                {% endfor %}
                </div>
            {% endif %}
        {% endwith %}

        {% block content %}{% endblock %}
    </div>

    <footer class="bg-white dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700 mt-auto">
        <div class="max-w-7xl mx-auto py-6 px-4 overflow-hidden sm:px-6 lg:px-8">
            <p class="text-center text-base text-gray-400">
                GNU General Public License v3.0
            </p>
        </div>
    </footer>
</body>
</html>
"""

SETUP_TEMPLATE = """
<div class="flex min-h-[80vh] items-center justify-center">
    <div class="w-full max-w-md bg-white dark:bg-gray-800 shadow-xl rounded-2xl overflow-hidden border border-gray-100 dark:border-gray-700">
        <div class="px-8 py-10">
            <div class="text-center mb-8">
                <h2 class="text-3xl font-bold text-gray-900 dark:text-white">Setup Admin</h2>
                <p class="text-sm text-gray-500 dark:text-gray-400 mt-2">Create your account to secure the bot. This account will be the <strong>Owner</strong>.</p>
            </div>
            <form method="post" class="space-y-6">
                <div>
                    <label for="username" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Username</label>
                    <input type="text" name="username" id="username" required class="appearance-none block w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white">
                </div>
                <div>
                    <label for="password" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Password</label>
                    <input type="password" name="password" id="password" required class="appearance-none block w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white">
                </div>
                <div>
                    <button type="submit" class="w-full flex justify-center py-2.5 px-4 border border-transparent rounded-lg shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 transition-colors">
                        Create Account
                    </button>
                </div>
            </form>
        </div>
    </div>
</div>
"""

LOGIN_TEMPLATE = """
<div class="flex min-h-[80vh] items-center justify-center">
    <div class="w-full max-w-md bg-white dark:bg-gray-800 shadow-xl rounded-2xl overflow-hidden border border-gray-100 dark:border-gray-700">
        <div class="px-8 py-10">
            <div class="text-center mb-8">
                <h2 class="text-3xl font-bold text-gray-900 dark:text-white">Welcome Back</h2>
                <p class="text-sm text-gray-500 dark:text-gray-400 mt-2">Please sign in to continue.</p>
            </div>
            <form method="post" class="space-y-6">
                <div>
                    <label for="username" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Username</label>
                    <input type="text" name="username" id="username" required class="appearance-none block w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white">
                </div>
                <div>
                    <label for="password" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Password</label>
                    <input type="password" name="password" id="password" required class="appearance-none block w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white">
                </div>
                <div>
                    <button type="submit" class="w-full flex justify-center py-2.5 px-4 border border-transparent rounded-lg shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 transition-colors">
                        Sign In
                    </button>
                </div>
            </form>
        </div>
    </div>
</div>
"""

VIEW_FEEDS_TEMPLATE = """
<!-- Stats Overview -->
<div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
    <div class="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-6 border border-gray-100 dark:border-gray-700">
        <div class="flex items-center">
            <div class="p-3 rounded-full bg-indigo-100 dark:bg-indigo-900 text-indigo-600 dark:text-indigo-300">
                <svg class="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path></svg>
            </div>
            <div class="ml-4">
                <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Total Feeds</p>
                <p class="text-2xl font-semibold text-gray-900 dark:text-white">{{ config.FEEDS|length }}</p>
            </div>
        </div>
    </div>
    <div class="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-6 border border-gray-100 dark:border-gray-700">
        <div class="flex items-center">
            <div class="p-3 rounded-full bg-green-100 dark:bg-green-900 text-green-600 dark:text-green-300">
                <svg class="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
            </div>
            <div class="ml-4">
                <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Webhook Status</p>
                {% set ns = namespace(active=0, paused=0) %}
                {% for feed in config.FEEDS %}
                    {% set count = feed.get('webhooks', [])|length %}
                    {% if feed.get('active', True) %}
                        {% set ns.active = ns.active + count %}
                    {% else %}
                        {% set ns.paused = ns.paused + count %}
                    {% endif %}
                {% endfor %}
                
                <div class="flex items-baseline space-x-1">
                    <span class="text-2xl font-semibold text-gray-900 dark:text-white">{{ ns.active }}</span>
                    <span class="text-sm text-gray-500 dark:text-gray-400">Active</span>
                </div>
                {% if ns.paused > 0 %}
                <div class="flex items-baseline space-x-1 mt-1">
                     <span class="text-sm font-semibold text-gray-400 dark:text-gray-500">{{ ns.paused }}</span>
                     <span class="text-xs text-gray-400 dark:text-gray-500">Paused</span>
                </div>
                {% endif %}
            </div>
        </div>
    </div>
    <div class="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-6 border border-gray-100 dark:border-gray-700 flex items-center justify-between">
         <div>
            <h3 class="text-lg font-medium text-gray-900 dark:text-white">Add New Subscription</h3>
            <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Connect a new RSS feed to Discord.</p>
         </div>
         <a href="{{ url_for('add_feed') }}" class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">
            + Add Feed
         </a>
    </div>
</div>

<!-- Feeds Table -->
<div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
    <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 flex justify-between items-center">
        <h3 class="text-lg font-semibold text-gray-900 dark:text-white">Active Feeds</h3>
    </div>
    <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead class="bg-gray-50 dark:bg-gray-700/50">
                <tr>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">RSS Name</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Last Post</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Details</th>
                    <th scope="col" class="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
                </tr>
            </thead>
            <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                {% for feed in config.FEEDS %}
                {% set state = feed_state.get(feed.id, {}) %}
                {% set status_code = state.get('status_code') %}
                {% set last_post = state.get('last_post', {}) %}
                {% set post_status = last_post.get('status') %}
                {% set post_time = last_post.get('timestamp') %}
                <tr class="hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors duration-150">
                    <td class="px-6 py-4 whitespace-nowrap">
                        {% if not feed.get('active', True) %}
                             <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-200 text-gray-800 dark:bg-gray-600 dark:text-gray-200">
                                 Paused
                             </span>
                        {% elif status_code %}
                            {% if 200 <= status_code < 300 %}
                                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                                    <span class="w-2 h-2 mr-1.5 bg-green-400 rounded-full"></span>
                                    {{ status_code }} OK
                                </span>
                            {% elif 300 <= status_code < 400 %}
                                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200">
                                    <span class="w-2 h-2 mr-1.5 bg-yellow-400 rounded-full"></span>
                                    {{ status_code }}
                                </span>
                            {% else %}
                                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200">
                                    <span class="w-2 h-2 mr-1.5 bg-red-400 rounded-full"></span>
                                    {{ status_code }} Error
                                </span>
                            {% endif %}
                        {% else %}
                             <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300">Pending</span>
                        {% endif %}
                    </td>
                    <td class="px-6 py-4">
                        <div class="text-sm font-medium text-gray-900 dark:text-white">{{ feed.get('name', 'Untitled Feed') }}</div>
                        <div class="text-xs text-gray-500 dark:text-gray-400 font-mono truncate max-w-[200px]" title="{{ feed.url }}">{{ feed.url }}</div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        {% if post_status %}
                             <div class="text-sm text-gray-900 dark:text-gray-200">{{ post_status }}</div>
                             <div class="text-xs text-gray-500">{{ time_ago(post_time) }}</div>
                        {% else %}
                            <span class="text-xs text-gray-400">No posts yet</span>
                        {% endif %}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <div class="text-xs text-gray-500 dark:text-gray-400">
                            <span class="font-medium">{{ feed.get('webhooks', [])|length }}</span> Webhooks
                        </div>
                        <div class="text-xs text-gray-500 dark:text-gray-400">
                            <span class="font-medium">{{ feed.update_interval }}s</span> Interval
                        </div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                        <div class="flex justify-end space-x-3 items-center">
                            <!-- Force Check Button -->
                             <form action="{{ url_for('force_check_feed', feed_id=feed.id) }}" method="post" class="inline">
                                <button type="submit" class="p-1 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors text-gray-400 hover:text-indigo-500" title="Force Check Now">
                                    <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z" clip-rule="evenodd" /></svg>
                                </button>
                            </form>

                            <!-- Pause/Resume Button -->
                            <form action="{{ url_for('toggle_pause_feed', feed_id=feed.id) }}" method="post" class="inline">
                                <button type="submit" class="p-1 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors {{ 'text-gray-400 hover:text-green-500' if not feed.get('active', True) else 'text-gray-400 hover:text-yellow-500' }}" title="{{ 'Resume' if not feed.get('active', True) else 'Pause' }}">
                                    {% if not feed.get('active', True) %}
                                        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM9.555 7.168A1 1 0 008 8v4a1 1 0 001.555.832l3-2a1 1 0 000-1.664l-3-2z" clip-rule="evenodd" /></svg>
                                    {% else %}
                                        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zM7 8a1 1 0 012 0v4a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v4a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd" /></svg>
                                    {% endif %}
                                </button>
                            </form>

                            <a href="{{ url_for('edit_feed', feed_id=feed.id) }}" class="text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300 p-1 rounded-md hover:bg-indigo-50 dark:hover:bg-gray-700">
                                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z" /></svg>
                            </a>
                            <form action="{{ url_for('delete_feed', feed_id=feed.id) }}" method="post" onsubmit="return confirm('Delete this feed? This cannot be undone.');" class="inline">
                                <button type="submit" class="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300 p-1 rounded-md hover:bg-red-50 dark:hover:bg-gray-700">
                                    <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd" /></svg>
                                </button>
                            </form>
                        </div>
                    </td>
                </tr>
                {% else %}
                <tr>
                    <td colspan="5" class="px-6 py-10 text-center text-gray-500 dark:text-gray-400">
                        <div class="mx-auto h-12 w-12 text-gray-300 dark:text-gray-600 mb-3">
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path></svg>
                        </div>
                        <p class="text-base font-medium">No feeds configured yet.</p>
                        <p class="text-sm mt-1">Get started by adding your first RSS feed.</p>
                        <div class="mt-4">
                            <a href="{{ url_for('add_feed') }}" class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700">Add Feed</a>
                        </div>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>
"""

FORM_TEMPLATE_SHARED_SCRIPT = """
<script>
    function addWebhookRow() {
        const container = document.getElementById('webhook-container');
        const newRow = document.createElement('div');
        newRow.className = 'flex items-center space-x-2 mb-2 animate-fade-in';
        newRow.innerHTML = `
            <div class="flex-grow relative rounded-md shadow-sm">
                <input type="url" name="webhook_url" placeholder="Webhook URL" class="focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md py-2 px-3" required>
            </div>
            <div class="w-1/3 relative rounded-md shadow-sm">
                <input type="text" name="webhook_label" placeholder="Label (e.g., Server - #channel)" class="focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md py-2 px-3">
            </div>
            <button type="button" onclick="this.parentElement.remove()" class="p-2 text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 hover:bg-red-50 dark:hover:bg-gray-700 rounded-md transition-colors" title="Remove">
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-trash" viewBox="0 0 16 16">
                    <path d="M5.5 5.5A.5.5 0 0 1 6 6v6a.5.5 0 0 1-1 0V6a.5.5 0 0 1 .5-.5zm2.5 0a.5.5 0 0 1 .5.5v6a.5.5 0 0 1-1 0V6a.5.5 0 0 1 .5-.5zm3 .5a.5.5 0 0 0-1 0v6a.5.5 0 0 0 1 0V6z"/>
                    <path fill-rule="evenodd" d="M14.5 3a1 1 0 0 1-1 1H13v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V4h-.5a1 1 0 0 1-1-1V2a1 1 0 0 1 1-1H6a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1h3.5a1 1 0 0 1 1 1v1zM4.118 4 4 4.059V13a1 1 0 0 0 1 1h6a1 1 0 0 0 1-1V4.059L11.882 4H4.118zM2.5 3V2h11v1h-11z"/>
                </svg>
            </button>
        `;
        container.appendChild(newRow);
    }
</script>
"""

ADD_FEED_TEMPLATE = """
<div class="max-w-2xl mx-auto">
    <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div class="px-6 py-5 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
            <h2 class="text-xl font-bold text-gray-900 dark:text-white">Add New Feed</h2>
            <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Configure a new RSS source and destinations.</p>
        </div>
        <form action="{{ url_for('add_feed') }}" method="post" class="p-6 space-y-6">
            <div class="grid grid-cols-1 gap-6">
                <div>
                    <label for="name" class="block text-sm font-medium text-gray-700 dark:text-gray-300">RSS Name (Optional)</label>
                    <input type="text" name="name" id="name" placeholder="e.g., Tech News" class="mt-1 block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                </div>

                <div>
                    <label for="url" class="block text-sm font-medium text-gray-700 dark:text-gray-300">RSS Feed URL</label>
                    <div class="mt-1 flex rounded-md shadow-sm">
                         <input type="url" name="url" id="url" placeholder="https://example.com/feed.xml" required class="flex-1 block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                    </div>
                </div>

                <div>
                    <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Webhook Destinations</label>
                    <div id="webhook-container" class="space-y-3">
                        <!-- Dynamic rows go here -->
                    </div>
                    <button type="button" onclick="addWebhookRow()" class="mt-2 inline-flex items-center px-3 py-1.5 border border-gray-300 dark:border-gray-600 shadow-sm text-xs font-medium rounded text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">
                        + Add Destination
                    </button>
                </div>

                <div>
                     <div class="flex items-center justify-between">
                         <label for="update_interval" class="block text-sm font-medium text-gray-700 dark:text-gray-300">Check Interval (seconds)</label>
                         <div class="flex items-center space-x-2">
                            <span class="text-sm text-gray-500 dark:text-gray-400">Active</span>
                            <label class="relative inline-flex items-center cursor-pointer">
                              <input type="checkbox" name="active" value="true" class="sr-only peer" checked>
                              <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-indigo-300 dark:peer-focus:ring-indigo-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-indigo-600"></div>
                            </label>
                         </div>
                     </div>
                     <input type="number" name="update_interval" id="update_interval" value="300" min="60" class="mt-1 block w-full sm:w-1/3 border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                </div>
            </div>

            <div class="pt-5 border-t border-gray-200 dark:border-gray-700 flex justify-end space-x-3">
                <a href="{{ url_for('view_feeds') }}" class="bg-white dark:bg-gray-700 py-2 px-4 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm text-sm font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">Cancel</a>
                <button type="submit" class="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">Save Feed</button>
            </div>
        </form>
    </div>
</div>
{{ FORM_TEMPLATE_SHARED_SCRIPT | safe }}
<script>document.addEventListener('DOMContentLoaded', function() { addWebhookRow(); });</script>
"""

EDIT_FEED_TEMPLATE = """
<div class="max-w-2xl mx-auto">
    <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div class="px-6 py-5 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 flex justify-between items-center">
            <h2 class="text-xl font-bold text-gray-900 dark:text-white">Edit Feed</h2>
        </div>
        <form action="{{ url_for('edit_feed', feed_id=feed.id) }}" method="post" class="p-6 space-y-6">
            <div class="grid grid-cols-1 gap-6">
                <div>
                    <label for="name" class="block text-sm font-medium text-gray-700 dark:text-gray-300">RSS Name (Optional)</label>
                    <input type="text" name="name" id="name" value="{{ feed.get('name', '') }}" class="mt-1 block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                </div>

                <div>
                    <label for="url" class="block text-sm font-medium text-gray-700 dark:text-gray-300">RSS Feed URL</label>
                    <input type="url" name="url" id="url" value="{{ feed.url }}" required class="mt-1 block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                </div>

                <div>
                    <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Webhook Destinations</label>
                    <div id="webhook-container" class="space-y-3">
                        {% for webhook in feed.get('webhooks', []) %}
                        <div class="flex items-center space-x-2 mb-3">
                            <div class="flex-grow relative rounded-md shadow-sm">
                                <input type="url" name="webhook_url" value="{{ webhook.url }}" placeholder="Webhook URL" class="focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md py-2 px-3" required>
                            </div>
                            <div class="w-1/3 relative rounded-md shadow-sm">
                                <input type="text" name="webhook_label" value="{{ webhook.label }}" placeholder="Label" class="focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md py-2 px-3">
                            </div>
                            <button type="button" onclick="this.parentElement.remove()" class="p-2 text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 hover:bg-red-50 dark:hover:bg-gray-700 rounded-md transition-colors" title="Remove">
                                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd" /></svg>
                            </button>
                        </div>
                        {% endfor %}
                    </div>
                    <button type="button" onclick="addWebhookRow()" class="mt-2 inline-flex items-center px-3 py-1.5 border border-gray-300 dark:border-gray-600 shadow-sm text-xs font-medium rounded text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">
                        + Add Destination
                    </button>
                </div>

                <div>
                     <div class="flex items-center justify-between">
                         <label for="update_interval" class="block text-sm font-medium text-gray-700 dark:text-gray-300">Check Interval (seconds)</label>
                         <div class="flex items-center space-x-2">
                            <span class="text-sm text-gray-500 dark:text-gray-400">Active</span>
                            <label class="relative inline-flex items-center cursor-pointer">
                              <input type="checkbox" name="active" value="true" class="sr-only peer" {{ 'checked' if feed.get('active', True) else '' }}>
                              <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-indigo-300 dark:peer-focus:ring-indigo-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-indigo-600"></div>
                            </label>
                         </div>
                     </div>
                     <input type="number" name="update_interval" id="update_interval" value="{{ feed.update_interval }}" min="60" class="mt-1 block w-full sm:w-1/3 border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                </div>
            </div>

            <div class="pt-5 border-t border-gray-200 dark:border-gray-700 flex justify-end space-x-3">
                <a href="{{ url_for('view_feeds') }}" class="bg-white dark:bg-gray-700 py-2 px-4 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm text-sm font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">Cancel</a>
                <button type="submit" class="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">Save Changes</button>
            </div>
        </form>
    </div>
</div>
{{ FORM_TEMPLATE_SHARED_SCRIPT | safe }}
"""

# User Management Templates

SETTINGS_TEMPLATE = """
<div class="max-w-6xl mx-auto grid grid-cols-1 gap-8">
    <!-- Change Password Card -->
    <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 p-6">
         <div class="mb-6 border-b border-gray-200 dark:border-gray-700 pb-4">
             <h3 class="text-lg font-bold text-gray-900 dark:text-white">My Profile</h3>
             <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Manage your account settings.</p>
         </div>
         <form action="{{ url_for('change_password') }}" method="post" class="space-y-4">
             <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                 <div>
                     <label for="current_password" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Current Password</label>
                     <input type="password" name="current_password" required class="block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                 </div>
                 <div>
                     <label for="new_password" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">New Password</label>
                     <input type="password" name="new_password" required class="block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                 </div>
             </div>
             <div class="flex justify-end">
                 <button type="submit" class="inline-flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">
                     Update Password
                 </button>
             </div>
         </form>
    </div>

    <!-- User Management Card -->
    <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 p-6">
         <div class="mb-6 border-b border-gray-200 dark:border-gray-700 pb-4 flex justify-between items-center">
             <div>
                 <h3 class="text-lg font-bold text-gray-900 dark:text-white">User Management</h3>
                 <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Manage access and permissions.</p>
             </div>
             {% if g.user.role in ['owner', 'super_admin'] %}
             <a href="{{ url_for('add_user') }}" class="text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300 text-sm font-medium inline-flex items-center">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" /></svg>
                Add User
             </a>
             {% endif %}
         </div>
         
         <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead class="bg-gray-50 dark:bg-gray-700/50">
                    <tr>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Username</th>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Role</th>
                        <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
                    </tr>
                </thead>
                <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                    {% for user in users %}
                    <tr class="group">
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 dark:text-white">
                            {{ user.username }}
                            {% if user.id == g.user.id %}
                                <span class="ml-2 px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">You</span>
                            {% endif %}
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap">
                            {% if user.role == 'owner' %}
                                <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200">Owner</span>
                            {% elif user.role == 'super_admin' %}
                                <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">Super Admin</span>
                            {% else %}
                                <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300">Admin</span>
                            {% endif %}
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium flex justify-end space-x-2 items-center">
                            {% if g.user.role == 'owner' and user.role != 'owner' and user.id %}
                                <!-- Owner Actions: Can do anything to anyone except self -->
                                <a href="{{ url_for('reset_password_page', user_id=user.id) }}" class="text-yellow-600 hover:text-yellow-900 dark:text-yellow-400 dark:hover:text-yellow-300 text-xs uppercase tracking-wide font-semibold px-2">Reset Pass</a>
                                
                                {% if user.role == 'admin' %}
                                <form action="{{ url_for('promote_user', user_id=user.id) }}" method="post" class="inline">
                                    <button type="submit" class="text-blue-600 hover:text-blue-900 dark:text-blue-400 dark:hover:text-blue-300 text-xs uppercase tracking-wide font-semibold px-2">Promote</button>
                                </form>
                                {% else %}
                                <form action="{{ url_for('demote_user', user_id=user.id) }}" method="post" class="inline">
                                    <button type="submit" class="text-gray-600 hover:text-gray-900 dark:text-gray-400 dark:hover:text-gray-300 text-xs uppercase tracking-wide font-semibold px-2">Demote</button>
                                </form>
                                {% endif %}
                                
                                <form action="{{ url_for('delete_user', user_id=user.id) }}" method="post" class="inline" onsubmit="return confirm('Are you sure you want to delete {{ user.username }}?');">
                                    <button type="submit" class="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300 hover:bg-red-50 dark:hover:bg-gray-700 p-1 rounded">
                                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd" /></svg>
                                    </button>
                                </form>
                            {% elif g.user.role == 'super_admin' and user.role == 'admin' and user.id %}
                                <!-- Super Admin Actions: Can manage admins only -->
                                <a href="{{ url_for('reset_password_page', user_id=user.id) }}" class="text-yellow-600 hover:text-yellow-900 dark:text-yellow-400 dark:hover:text-yellow-300 text-xs uppercase tracking-wide font-semibold px-2">Reset Pass</a>

                                <form action="{{ url_for('delete_user', user_id=user.id) }}" method="post" class="inline" onsubmit="return confirm('Are you sure you want to delete {{ user.username }}?');">
                                    <button type="submit" class="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300 hover:bg-red-50 dark:hover:bg-gray-700 p-1 rounded">
                                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd" /></svg>
                                    </button>
                                </form>
                            {% else %}
                                <span class="text-gray-400 dark:text-gray-600 text-xs italic">No actions</span>
                            {% endif %}
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
         </div>
    </div>
</div>
"""

ADD_USER_TEMPLATE = """
<div class="max-w-2xl mx-auto">
    <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div class="px-6 py-5 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
            <h2 class="text-xl font-bold text-gray-900 dark:text-white">Add New User</h2>
            <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Create a new administrator account.</p>
        </div>
        <form action="{{ url_for('add_user') }}" method="post" class="p-6 space-y-6">
            <div class="grid grid-cols-1 gap-6">
                <div>
                    <label for="username" class="block text-sm font-medium text-gray-700 dark:text-gray-300">Username</label>
                    <input type="text" name="username" id="username" required class="mt-1 block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                </div>
                <div>
                    <label for="password" class="block text-sm font-medium text-gray-700 dark:text-gray-300">Password</label>
                    <input type="password" name="password" id="password" required class="mt-1 block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                </div>
                
                {% if g.user.role == 'owner' %}
                <div>
                    <label for="role" class="block text-sm font-medium text-gray-700 dark:text-gray-300">Role</label>
                    <select name="role" id="role" class="mt-1 block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
                        <option value="admin">Admin</option>
                        <option value="super_admin">Super Admin</option>
                    </select>
                    <p class="mt-1 text-xs text-gray-500">Super Admins can delete other Admins.</p>
                </div>
                {% else %}
                <!-- Super Admin creates regular Admins by default, hide selector -->
                <input type="hidden" name="role" value="admin">
                {% endif %}
            </div>

            <div class="pt-5 border-t border-gray-200 dark:border-gray-700 flex justify-end space-x-3">
                <a href="{{ url_for('settings') }}" class="bg-white dark:bg-gray-700 py-2 px-4 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm text-sm font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">Cancel</a>
                <button type="submit" class="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">Create User</button>
            </div>
        </form>
    </div>
</div>
"""

RESET_PASSWORD_TEMPLATE = """
<div class="max-w-md mx-auto">
    <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div class="px-6 py-5 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
            <h2 class="text-xl font-bold text-gray-900 dark:text-white">Reset User Password</h2>
            <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Force update password for <strong>{{ target_user.username }}</strong>.</p>
        </div>
        <form action="{{ url_for('force_reset_password', user_id=target_user.id) }}" method="post" class="p-6 space-y-6">
            <div>
                <label for="new_password" class="block text-sm font-medium text-gray-700 dark:text-gray-300">New Password</label>
                <input type="password" name="new_password" id="new_password" required class="mt-1 block w-full border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm py-2 px-3">
            </div>

            <div class="pt-5 border-t border-gray-200 dark:border-gray-700 flex justify-end space-x-3">
                <a href="{{ url_for('settings') }}" class="bg-white dark:bg-gray-700 py-2 px-4 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm text-sm font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">Cancel</a>
                <button type="submit" class="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">Update Password</button>
            </div>
        </form>
    </div>
</div>
"""

BACKUP_RESTORE_TEMPLATE = """
<div class="max-w-4xl mx-auto space-y-12">
    <!-- Config Section -->
    <div>
        <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">Feed Configuration</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
             <!-- Backup Card -->
            <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 p-6 flex flex-col">
                <div class="mb-4">
                     <div class="h-12 w-12 bg-indigo-100 dark:bg-indigo-900 rounded-lg flex items-center justify-center text-indigo-600 dark:text-indigo-300 mb-4">
                        <svg class="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                     </div>
                     <h3 class="text-lg font-bold text-gray-900 dark:text-white">Download Config</h3>
                     <p class="mt-2 text-sm text-gray-500 dark:text-gray-400">Export your entire feed configuration to a JSON file. Use this to migrate servers or keep a safe copy.</p>
                </div>
                <div class="mt-auto">
                    <a href="{{ url_for('download_backup') }}" class="w-full flex justify-center items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500">
                        Download config.json
                    </a>
                </div>
            </div>

            <!-- Restore Card -->
            <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 p-6 flex flex-col">
                <div class="mb-4">
                     <div class="h-12 w-12 bg-green-100 dark:bg-green-900 rounded-lg flex items-center justify-center text-green-600 dark:text-green-300 mb-4">
                        <svg class="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"></path></svg>
                     </div>
                     <h3 class="text-lg font-bold text-gray-900 dark:text-white">Restore Config</h3>
                     <p class="mt-2 text-sm text-gray-500 dark:text-gray-400">Restore feeds from a backup file. <span class="font-semibold text-red-500">Warning: This will replace all current feeds.</span></p>
                </div>
                <form action="{{ url_for('upload_backup') }}" method="post" enctype="multipart/form-data" class="mt-auto">
                    <div class="mt-1 flex justify-center px-6 pt-5 pb-6 border-2 border-gray-300 dark:border-gray-600 border-dashed rounded-md hover:border-indigo-500 transition-colors cursor-pointer" onclick="document.getElementById('file-upload').click()">
                        <div class="space-y-1 text-center">
                            <svg class="mx-auto h-12 w-12 text-gray-400" stroke="currentColor" fill="none" viewBox="0 0 48 48" aria-hidden="true">
                                <path d="M28 8H12a4 4 0 00-4 4v20m32-12v8m0 0v8a4 4 0 01-4 4H12a4 4 0 01-4-4v-4m32-4l-3.172-3.172a4 4 0 00-5.656 0L28 28M8 32l9.172-9.172a4 4 0 015.656 0L28 28m0 0l4 4m4-24h8m-4-4v8m-12 4h.02" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
                            </svg>
                            <div class="flex text-sm text-gray-600 dark:text-gray-400 justify-center">
                                <label for="file-upload" class="relative cursor-pointer bg-transparent rounded-md font-medium text-indigo-600 dark:text-indigo-400 hover:text-indigo-500 focus-within:outline-none">
                                    <span>Upload config.json</span>
                                    <input id="file-upload" name="backup_file" type="file" accept=".json" class="sr-only" onchange="this.form.submit()">
                                </label>
                            </div>
                            <p class="text-xs text-gray-500 dark:text-gray-400">JSON files only</p>
                        </div>
                    </div>
                </form>
            </div>
        </div>
    </div>

    <!-- User Section (Owner Only) -->
    {% if g.user.role == 'owner' %}
    <div class="border-t border-gray-200 dark:border-gray-700 pt-12">
        <div class="flex items-center justify-between mb-6">
             <h2 class="text-2xl font-bold text-gray-900 dark:text-white">User Database</h2>
             <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200">Owner Access Only</span>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
             <!-- User Download Card -->
            <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 p-6 flex flex-col">
                <div class="mb-4">
                     <div class="h-12 w-12 bg-purple-100 dark:bg-purple-900 rounded-lg flex items-center justify-center text-purple-600 dark:text-purple-300 mb-4">
                        <svg class="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"></path></svg>
                     </div>
                     <h3 class="text-lg font-bold text-gray-900 dark:text-white">Download User DB</h3>
                     <p class="mt-2 text-sm text-gray-500 dark:text-gray-400">Export the full user database, including hashed passwords and roles. Handle with care.</p>
                </div>
                <div class="mt-auto">
                    <a href="{{ url_for('download_users_backup') }}" class="w-full flex justify-center items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-purple-600 hover:bg-purple-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500">
                        Download user.json
                    </a>
                </div>
            </div>

            <!-- User Restore Card -->
            <div class="bg-white dark:bg-gray-800 shadow-lg rounded-xl border border-gray-200 dark:border-gray-700 p-6 flex flex-col">
                <div class="mb-4">
                     <div class="h-12 w-12 bg-red-100 dark:bg-red-900 rounded-lg flex items-center justify-center text-red-600 dark:text-red-300 mb-4">
                        <svg class="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
                     </div>
                     <h3 class="text-lg font-bold text-gray-900 dark:text-white">Restore User DB</h3>
                     <p class="mt-2 text-sm text-gray-500 dark:text-gray-400">Restore users from backup. <span class="font-semibold text-red-500">Warning: Replaces ALL users immediately.</span></p>
                </div>
                <form action="{{ url_for('upload_users_backup') }}" method="post" enctype="multipart/form-data" class="mt-auto">
                    <div class="mt-1 flex justify-center px-6 pt-5 pb-6 border-2 border-gray-300 dark:border-gray-600 border-dashed rounded-md hover:border-purple-500 transition-colors cursor-pointer" onclick="document.getElementById('user-file-upload').click()">
                        <div class="space-y-1 text-center">
                            <svg class="mx-auto h-12 w-12 text-gray-400" stroke="currentColor" fill="none" viewBox="0 0 48 48" aria-hidden="true">
                                <path d="M28 8H12a4 4 0 00-4 4v20m32-12v8m0 0v8a4 4 0 01-4 4H12a4 4 0 01-4-4v-4m32-4l-3.172-3.172a4 4 0 00-5.656 0L28 28M8 32l9.172-9.172a4 4 0 015.656 0L28 28m0 0l4 4m4-24h8m-4-4v8m-12 4h.02" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
                            </svg>
                            <div class="flex text-sm text-gray-600 dark:text-gray-400 justify-center">
                                <label for="user-file-upload" class="relative cursor-pointer bg-transparent rounded-md font-medium text-purple-600 dark:text-purple-400 hover:text-purple-500 focus-within:outline-none">
                                    <span>Upload user.json</span>
                                    <input id="user-file-upload" name="backup_file" type="file" accept=".json" class="sr-only" onchange="this.form.submit()">
                                </label>
                            </div>
                            <p class="text-xs text-gray-500 dark:text-gray-400">JSON files only</p>
                        </div>
                    </div>
                </form>
            </div>
        </div>
    </div>
    {% endif %}
</div>
"""

# This dictionary will act as a simple template loader.
# Moved to the end to ensure all variables are defined.
TEMPLATES = {
    "layout": LAYOUT_TEMPLATE,
    "view_feeds": VIEW_FEEDS_TEMPLATE,
    "add_feed": ADD_FEED_TEMPLATE,
    "edit_feed": EDIT_FEED_TEMPLATE,
    "backup_restore": BACKUP_RESTORE_TEMPLATE,
    "setup": SETUP_TEMPLATE,
    "login": LOGIN_TEMPLATE,
    "settings": SETTINGS_TEMPLATE,
    "add_user": ADD_USER_TEMPLATE,
    "reset_password": RESET_PASSWORD_TEMPLATE,
    "form_shared_script": FORM_TEMPLATE_SHARED_SCRIPT
}

# --- Configuration and State Management ---

def save_config(config_data):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config_data, f, indent=4)

def initialize_files():
    """Ensure all necessary files exist before the app starts."""
    if not os.path.exists(CONFIG_FILE):
        save_config({"FEEDS": []})
    if not os.path.exists(SENT_ARTICLES_FILE):
        with open(SENT_ARTICLES_FILE, 'w') as f: yaml.dump([], f)
    if not os.path.exists(FEED_STATE_FILE):
        with open(FEED_STATE_FILE, 'w') as f: json.dump({}, f)
    # User file is checked separately by the auth logic

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def load_feed_state():
    try:
        with open(FEED_STATE_FILE, 'r') as f:
            content = f.read()
            if not content: return {}
            return json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

# --- Initialize files on application startup ---
initialize_files()

# --- Authentication Logic ---

def get_secret_key():
    """Generates a secret key and saves it, or loads the existing one."""
    if not os.path.exists(SECRET_KEY_FILE):
        print("Generating new secret key...")
        key = os.urandom(24)
        with open(SECRET_KEY_FILE, 'wb') as f:
            f.write(key)
        return key
    else:
        with open(SECRET_KEY_FILE, 'rb') as f:
            return f.read()

app.secret_key = get_secret_key()

def admin_user_exists():
    return os.path.exists(USER_FILE)

def get_users():
    """Safely loads all users from the JSON file."""
    if not os.path.exists(USER_FILE):
        return []
    try:
        with open(USER_FILE, 'r') as f:
            content = f.read()
            if not content:
                return []
            data = json.loads(content)
            # Migrate single user object to list if necessary
            if isinstance(data, dict):
                data = [data]
                with open(USER_FILE, 'w') as f:
                     json.dump(data, f)
            return data
    except (json.JSONDecodeError, FileNotFoundError):
        return []

def save_users(users):
    with open(USER_FILE, 'w') as f:
        json.dump(users, f)

def get_user_by_id(user_id):
    users = get_users()
    return next((user for user in users if user['id'] == user_id), None)

@app.before_request
def load_logged_in_user():
    user_id = session.get('user_id')
    g.user = get_user_by_id(user_id) if user_id else None
    g.now_year = datetime.now().year

@app.before_request
def require_login_or_setup():
    if not admin_user_exists() and request.endpoint != 'setup':
        return redirect(url_for('setup'))
    
    if admin_user_exists() and g.user is None and request.endpoint not in ['login', 'setup']:
        return redirect(url_for('login'))

# --- Flask Routes ---

@app.route('/setup', methods=['GET', 'POST'])
def setup():
    if admin_user_exists():
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        user_data = [{
            "id": str(uuid.uuid4()),
            "username": username,
            "password": generate_password_hash(password),
            "role": "owner" # First user is always owner
        }]
        with open(USER_FILE, 'w') as f:
            json.dump(user_data, f)
        
        flash('Admin account created successfully! Please log in.', 'success')
        return redirect(url_for('login'))

    full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["setup"])
    return render_template_string(full_html)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if g.user:
        return redirect(url_for('view_feeds'))
    
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        users = get_users()
        user = next((u for u in users if u['username'] == username), None)
        
        if user and check_password_hash(user.get('password', ''), password):
            session.clear()
            session['user_id'] = user['id']
            return redirect(url_for('view_feeds'))
        else:
            flash("Invalid username or password.", 'error')

    full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["login"])
    return render_template_string(full_html)

@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.', 'success')
    return redirect(url_for('login'))

@app.route('/')
def view_feeds():
    config = load_config()
    feed_state = load_feed_state()
    full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["view_feeds"])
    return render_template_string(full_html, config=config, feed_state=feed_state)

@app.route('/add', methods=['GET', 'POST'])
def add_feed():
    if request.method == 'POST':
        config = load_config()
        webhook_urls = request.form.getlist('webhook_url')
        webhook_labels = request.form.getlist('webhook_label')
        active_status = request.form.get('active') == 'true'
        
        webhooks_data = [
            {"url": url, "label": label}
            for url, label in zip(webhook_urls, webhook_labels) if url
        ]

        new_feed = {
            "id": str(uuid.uuid4()),
            "name": request.form['name'],
            "url": request.form['url'],
            "webhooks": webhooks_data,
            "update_interval": int(request.form['update_interval']),
            "active": active_status
        }
        config['FEEDS'].append(new_feed)
        save_config(config)
        flash(f'Feed "{new_feed["url"]}" added successfully!', 'success')
        return redirect(url_for('view_feeds'))
    
    full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["add_feed"])
    return render_template_string(full_html, FORM_TEMPLATE_SHARED_SCRIPT=TEMPLATES["form_shared_script"])

@app.route('/edit/<feed_id>', methods=['GET', 'POST'])
def edit_feed(feed_id):
    config = load_config()
    feed_to_edit = next((feed for feed in config['FEEDS'] if feed['id'] == feed_id), None)

    if feed_to_edit is None:
        flash('Feed not found.', 'error')
        return redirect(url_for('view_feeds'))

    # Handle legacy format for backward compatibility
    if 'webhook_urls' in feed_to_edit and 'webhooks' not in feed_to_edit:
        feed_to_edit['webhooks'] = [{"url": url, "label": ""} for url in feed_to_edit['webhook_urls']]

    if request.method == 'POST':
        webhook_urls = request.form.getlist('webhook_url')
        webhook_labels = request.form.getlist('webhook_label')
        active_status = request.form.get('active') == 'true'
        
        webhooks_data = [
            {"url": url, "label": label}
            for url, label in zip(webhook_urls, webhook_labels) if url
        ]

        for i, feed in enumerate(config['FEEDS']):
            if feed['id'] == feed_id:
                config['FEEDS'][i]['name'] = request.form['name']
                config['FEEDS'][i]['url'] = request.form['url']
                config['FEEDS'][i]['webhooks'] = webhooks_data
                config['FEEDS'][i]['update_interval'] = int(request.form['update_interval'])
                config['FEEDS'][i]['active'] = active_status
                # Clean up legacy fields if they exist
                config['FEEDS'][i].pop('webhook_url', None)
                config['FEEDS'][i].pop('webhook_urls', None)
                break
        
        save_config(config)
        flash(f'Feed updated successfully!', 'success')
        return redirect(url_for('view_feeds'))

    full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["edit_feed"])
    return render_template_string(full_html, feed=feed_to_edit, FORM_TEMPLATE_SHARED_SCRIPT=TEMPLATES["form_shared_script"])

@app.route('/delete/<feed_id>', methods=['POST'])
def delete_feed(feed_id):
    config = load_config()
    feed_to_delete = next((feed for feed in config['FEEDS'] if feed['id'] == feed_id), None)
    if feed_to_delete:
        config['FEEDS'] = [feed for feed in config['FEEDS'] if feed['id'] != feed_id]
        save_config(config)
        flash(f'Feed "{feed_to_delete["url"]}" deleted.', 'success')
    else:
        flash('Feed not found.', 'error')
    return redirect(url_for('view_feeds'))

@app.route('/toggle_pause/<feed_id>', methods=['POST'])
def toggle_pause_feed(feed_id):
    config = load_config()
    for i, feed in enumerate(config['FEEDS']):
        if feed['id'] == feed_id:
            current_status = feed.get('active', True)
            config['FEEDS'][i]['active'] = not current_status
            save_config(config)
            status_msg = "resumed" if not current_status else "paused"
            flash(f'Feed {status_msg}.', 'success')
            return redirect(url_for('view_feeds'))
    
    flash('Feed not found.', 'error')
    return redirect(url_for('view_feeds'))

@app.route('/force_check/<feed_id>', methods=['POST'])
def force_check_feed(feed_id):
    # 1. Load feed config
    config = load_config()
    feed_config = next((f for f in config['FEEDS'] if f['id'] == feed_id), None)
    
    if not feed_config:
        flash('Feed not found.', 'error')
        return redirect(url_for('view_feeds'))

    # 2. Import scheduler logic
    check_single_feed, load_feed_state_func, save_feed_state_func = get_scheduler_check_function()
    
    if not check_single_feed:
        flash('Error: Could not load scheduler module.', 'error')
        return redirect(url_for('view_feeds'))

    # 3. Run the check immediately
    try:
        # We need to load the current state to pass it in (required by the new scheduler logic)
        current_state = load_feed_state_func()
        
        # Run the check
        status_code, last_post_status = check_single_feed(feed_config, current_state)
        
        # 4. Update the state file with the result so the UI updates immediately
        now = datetime.now(timezone.utc)
        
        if feed_id not in current_state:
            current_state[feed_id] = {}
            
        current_state[feed_id]['status_code'] = status_code
        current_state[feed_id]['last_checked'] = now.isoformat()
        
        if last_post_status:
            current_state[feed_id]['last_post'] = {
                "status": last_post_status,
                "timestamp": now.isoformat()
            }
            
        save_feed_state_func(current_state)
        
        flash(f'Feed checked successfully. Status: {status_code}', 'success')
        
    except Exception as e:
        flash(f'Error checking feed: {e}', 'error')

    return redirect(url_for('view_feeds'))

@app.route('/settings')
def settings():
    users = get_users()
    full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["settings"])
    return render_template_string(full_html, users=users)

@app.route('/settings/change-password', methods=['POST'])
def change_password():
    current_password = request.form['current_password']
    new_password = request.form['new_password']
    
    users = get_users()
    # Since g.user is a copy, we need to find the user in the list to modify it
    user_index = next((i for i, u in enumerate(users) if u['id'] == g.user['id']), -1)
    
    if user_index == -1:
         flash("User not found.", "error")
         return redirect(url_for('settings'))

    user = users[user_index]

    if not check_password_hash(user['password'], current_password):
        flash("Incorrect current password.", "error")
        return redirect(url_for('settings'))
    
    users[user_index]['password'] = generate_password_hash(new_password)
    save_users(users)
    
    flash("Password updated successfully.", "success")
    return redirect(url_for('settings'))

@app.route('/settings/users/add', methods=['GET', 'POST'])
def add_user():
    if g.user['role'] not in ['owner', 'super_admin']:
        flash("You do not have permission to create users.", "error")
        return redirect(url_for('settings'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        role = request.form.get('role', 'admin') # Default to admin
        
        # Only owner can create super admins
        if role == 'super_admin' and g.user['role'] != 'owner':
            role = 'admin'

        users = get_users()
        if any(u['username'] == username for u in users):
            flash("Username already exists.", "error")
            return redirect(url_for('add_user'))
            
        new_user = {
            "id": str(uuid.uuid4()),
            "username": username,
            "password": generate_password_hash(password),
            "role": role
        }
        users.append(new_user)
        save_users(users)
        
        flash(f"User '{username}' created successfully.", "success")
        return redirect(url_for('settings'))

    full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["add_user"])
    return render_template_string(full_html)

@app.route('/settings/users/promote/<user_id>', methods=['POST'])
def promote_user(user_id):
    if g.user['role'] != 'owner':
        flash("Only the Owner can promote users.", "error")
        return redirect(url_for('settings'))
        
    users = get_users()
    target_user = next((u for u in users if u['id'] == user_id), None)
    
    if target_user and target_user['role'] == 'admin':
        target_user['role'] = 'super_admin'
        save_users(users)
        flash(f"User promoted to Super Admin.", "success")
    
    return redirect(url_for('settings'))

@app.route('/settings/users/demote/<user_id>', methods=['POST'])
def demote_user(user_id):
    if g.user['role'] != 'owner':
        flash("Only the Owner can demote users.", "error")
        return redirect(url_for('settings'))
        
    users = get_users()
    target_user = next((u for u in users if u['id'] == user_id), None)
    
    if target_user and target_user['role'] == 'super_admin':
        target_user['role'] = 'admin'
        save_users(users)
        flash(f"User demoted to Admin.", "success")
    
    return redirect(url_for('settings'))

@app.route('/settings/users/reset-password/<user_id>', methods=['GET', 'POST'])
def reset_password_page(user_id):
    if g.user['role'] not in ['owner', 'super_admin']:
        flash("You do not have permission to reset passwords.", "error")
        return redirect(url_for('settings'))
        
    users = get_users()
    target_user = next((u for u in users if u['id'] == user_id), None)
    
    if not target_user:
        flash("User not found.", "error")
        return redirect(url_for('settings'))

    # Super Admin check: Can only reset admins
    if g.user['role'] == 'super_admin' and target_user.get('role') != 'admin':
        flash("Super Admins can only reset passwords for Admins.", "error")
        return redirect(url_for('settings'))
        
    if request.method == 'GET':
        full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["reset_password"])
        return render_template_string(full_html, target_user=target_user)

@app.route('/settings/users/force-reset-password/<user_id>', methods=['POST'])
def force_reset_password(user_id):
    if g.user['role'] not in ['owner', 'super_admin']:
        flash("You do not have permission to reset passwords.", "error")
        return redirect(url_for('settings'))
        
    new_password = request.form['new_password']
    
    users = get_users()
    user_index = next((i for i, u in enumerate(users) if u['id'] == user_id), -1)
    
    if user_index != -1:
        target_user = users[user_index]
        
        # Super Admin check: Can only reset admins
        if g.user['role'] == 'super_admin' and target_user.get('role') != 'admin':
            flash("Super Admins can only reset passwords for Admins.", "error")
            return redirect(url_for('settings'))

        users[user_index]['password'] = generate_password_hash(new_password)
        save_users(users)
        flash("Password forcefully updated.", "success")
    else:
        flash("User not found.", "error")
        
    return redirect(url_for('settings'))

@app.route('/settings/users/delete/<user_id>', methods=['POST'])
def delete_user(user_id):
    if user_id == g.user['id']:
        flash("You cannot delete your own account.", "error")
        return redirect(url_for('settings'))
    
    users = get_users()
    target_user = next((u for u in users if u['id'] == user_id), None)
    
    if not target_user:
        flash("User not found.", "error")
        return redirect(url_for('settings'))

    # PERMISSION CHECKS
    
    # 1. No one can delete the Owner
    if target_user.get('role') == 'owner':
        flash("The Owner cannot be deleted.", "error")
        return redirect(url_for('settings'))

    # 2. Owner can delete anyone (except themselves, handled above)
    if g.user['role'] == 'owner':
        users = [u for u in users if u['id'] != user_id]
        save_users(users)
        flash("User deleted successfully.", "success")
        return redirect(url_for('settings'))

    # 3. Super Admin can delete Admins only
    if g.user['role'] == 'super_admin':
        if target_user.get('role') == 'admin':
            users = [u for u in users if u['id'] != user_id]
            save_users(users)
            flash("User deleted successfully.", "success")
        else:
            flash("Super Admins cannot delete other Super Admins or Owners.", "error")
        return redirect(url_for('settings'))

    # 4. Admins cannot delete anyone
    flash("You do not have permission to delete users.", "error")
    return redirect(url_for('settings'))

@app.route('/backup-restore')
def backup_restore():
    full_html = TEMPLATES["layout"].replace('{% block content %}{% endblock %}', TEMPLATES["backup_restore"])
    return render_template_string(full_html)

@app.route('/backup/download')
def download_backup():
    return send_file(CONFIG_FILE, as_attachment=True)

@app.route('/backup/users/download')
def download_users_backup():
    # Only Owner can download the user database
    if g.user.get('role') != 'owner':
        flash('Only the Owner can download user backups.', 'error')
        return redirect(url_for('backup_restore'))
    
    if not os.path.exists(USER_FILE):
        flash('User database not found.', 'error')
        return redirect(url_for('backup_restore'))
        
    return send_file(USER_FILE, as_attachment=True)

@app.route('/backup/upload', methods=['POST'])
def upload_backup():
    if 'backup_file' not in request.files:
        flash('No file part in the request.', 'error')
        return redirect(url_for('backup_restore'))
    
    file = request.files['backup_file']
    if file.filename == '':
        flash('No file selected for uploading.', 'error')
        return redirect(url_for('backup_restore'))

    if file and file.filename.endswith('.json'):
        try:
            content = file.read().decode('utf-8')
            # Basic validation
            data = json.loads(content)
            if 'FEEDS' not in data:
                raise ValueError("Invalid config file: 'FEEDS' key is missing.")
            
            with open(CONFIG_FILE, 'w') as f:
                f.write(content)
            flash('Configuration restored successfully! Please restart the scheduler service for changes to take full effect.', 'success')
        except Exception as e:
            flash(f'Error processing file: {e}', 'error')
    else:
        flash('Invalid file type. Please upload a .json file.', 'error')

    return redirect(url_for('backup_restore'))

@app.route('/backup/users/upload', methods=['POST'])
def upload_users_backup():
    # Only Owner can restore user backups
    if g.user.get('role') != 'owner':
        flash('Only the Owner can restore user backups.', 'error')
        return redirect(url_for('backup_restore'))

    if 'backup_file' not in request.files:
        flash('No file part in the request.', 'error')
        return redirect(url_for('backup_restore'))
    
    file = request.files['backup_file']
    if file.filename == '':
        flash('No file selected for uploading.', 'error')
        return redirect(url_for('backup_restore'))

    if file and file.filename.endswith('.json'):
        try:
            content = file.read().decode('utf-8')
            users_data = json.loads(content)
            
            # Validate structure: must be a list of user objects
            if not isinstance(users_data, list):
                # Handle single object case from very old versions
                if isinstance(users_data, dict) and 'username' in users_data:
                    users_data = [users_data]
                else:
                    raise ValueError("Invalid user file format. Expected a list of users.")

            # Validate required fields
            for u in users_data:
                if not all(k in u for k in ('username', 'password', 'id')):
                    raise ValueError(f"User entry missing required fields: {u.get('username', 'unknown')}")
                # Ensure role exists
                if 'role' not in u:
                    u['role'] = 'admin'

            # Ensure at least one owner exists in the backup
            if not any(u.get('role') == 'owner' for u in users_data):
                # Safety check: promote the first user to owner if none exist
                users_data[0]['role'] = 'owner'
                
            # Save file
            with open(USER_FILE, 'w') as f:
                json.dump(users_data, f, indent=4)
                
            flash('User database restored successfully. You may need to log in again.', 'success')
            
        except Exception as e:
            flash(f'Error restoring users: {e}', 'error')
    else:
        flash('Invalid file type. Please upload a .json file.', 'error')

    return redirect(url_for('backup_restore'))


if __name__ == "__main__":
    print("Registered Routes:")
    for rule in app.url_map.iter_rules():
        print(f"{rule.endpoint}: {rule.rule}")
    print("\nThis script is for the web UI and is not meant to be run directly for production.")
    print("Use Gunicorn to serve the 'app' object in this file.")
    print("Example: gunicorn --bind 0.0.0.0:5000 main_web:app")
    app.run(host='0.0.0.0', port=5000, debug=True)
