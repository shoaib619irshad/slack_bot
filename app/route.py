from flask import Blueprint

from main import feeds, ProcessRSSFeed

slack_bp = Blueprint('slack_msg',__name__)

@slack_bp.route('/',methods=["POST"])
def slack_bot():
    ProcessRSSFeed(feeds)