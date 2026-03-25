"""
LearnDash Data Processing Functions
====================================

Simple functions to fetch and process data from WordPress API.

Usage:
    from learndash_functions import *
    
    # Get webinar registrations
    registrations = get_webinar_registrations()
    
    # Get course summary
    summary = get_course_summary(9204)
    
    # Get course breakdown
    breakdown = get_course_breakdown(9204)
"""

import requests
from typing import Dict, List, Any, Optional


# Configuration
WORDPRESS_URL = "https://esapa.one/"
API_SECRET = "sanyam1212"  # Must match PHP file


def _fetch(action: str, params: Optional[Dict] = None) -> Dict:
    """Internal function to fetch data from WordPress API"""
    if params is None:
        params = {}
    
    params['ld_api'] = action
    params['secret'] = API_SECRET
    
    response = requests.get(WORDPRESS_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


# =============================================================================
# WEBINAR FUNCTIONS
# =============================================================================

def get_webinar_registrations() -> List[Dict]:
    """
    Get all webinar registrations with event details and dates.
    Data includes: registration info, event title/status, and event dates/times.
    
    Returns:
        List of registration dictionaries with joined event data
    """
    result = _fetch('webinar_registrations')
    return result.get('data', [])


def filter_registrations_by_title(title: str, registrations: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Filter registrations by event title.
    
    Args:
        title: Event title to match (case-insensitive, partial match)
        registrations: Optional list of registrations. If None, fetches fresh data.
    
    Returns:
        List of matching registrations
    """
    if registrations is None:
        registrations = get_webinar_registrations()
    
    title_lower = title.lower()
    return [r for r in registrations if title_lower in (r.get('event_title') or '').lower()]


def get_sapa_growth_clinics_registrations() -> List[Dict]:
    """
    Get all registrations for "SAPA Growth Clinics - Introduction & Q&A" events.
    
    Returns:
        List of registrations for SAPA Growth Clinics events
    """
    return filter_registrations_by_title('SAPA Growth Clinics - Introduction & Q&A')


def count_registrations_by_event(registrations: Optional[List[Dict]] = None) -> Dict[int, Dict]:
    """
    Count registrations grouped by event with event details.
    
    Args:
        registrations: Optional list of registrations. If None, fetches fresh data.
    
    Returns:
        Dictionary mapping event_id to {title, count, start_date, end_date}
    """
    if registrations is None:
        registrations = get_webinar_registrations()
    
    counts = {}
    for reg in registrations:
        event_id = reg.get('event_id')
        if event_id not in counts:
            counts[event_id] = {
                'title': reg.get('event_title'),
                'count': 0,
                'start_date': reg.get('start_date'),
                'end_date': reg.get('end_date'),
                'timezone': reg.get('timezone'),
            }
        counts[event_id]['count'] += 1
    
    return counts


def get_registrations_by_date_range(start_date: str, end_date: str, 
                                    registrations: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Get registrations for events within a date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format)
        registrations: Optional list of registrations. If None, fetches fresh data.
    
    Returns:
        List of registrations for events in date range
    """
    if registrations is None:
        registrations = get_webinar_registrations()
    
    filtered = []
    for reg in registrations:
        event_start = reg.get('start_date', '')[:10] if reg.get('start_date') else ''
        if event_start and start_date <= event_start <= end_date:
            filtered.append(reg)
    
    return filtered


def get_registrations_by_email(email: str, registrations: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Find all registrations for a specific email.
    
    Args:
        email: Email address to search for
        registrations: Optional list of registrations. If None, fetches fresh data.
    
    Returns:
        List of matching registrations
    """
    if registrations is None:
        registrations = get_webinar_registrations()
    
    return [r for r in registrations if (r.get('email') or '').lower() == email.lower()]


# =============================================================================
# COURSE SUMMARY FUNCTIONS
# =============================================================================

def get_course_summary(course_id: int = 9204) -> Dict:
    """
    Get enrollment summary for a course.
    
    Args:
        course_id: LearnDash course post ID
    
    Returns:
        Dictionary with total_enrolled, completed, in_progress, not_started
    """
    result = _fetch('course_summary', {'course_id': course_id})
    return result.get('data', {})


def calculate_completion_rate(course_id: int = 9204) -> float:
    """
    Calculate course completion rate as percentage.
    
    Args:
        course_id: LearnDash course post ID
    
    Returns:
        Completion rate (0-100)
    """
    summary = get_course_summary(course_id)
    total = int(summary.get('total_enrolled', 0))
    completed = int(summary.get('completed', 0))
    
    if total == 0:
        return 0.0
    
    return round((completed / total) * 100, 2)


def get_course_stats(course_id: int = 9204) -> Dict:
    """
    Get comprehensive course statistics.
    
    Args:
        course_id: LearnDash course post ID
    
    Returns:
        Dictionary with counts and percentages
    """
    summary = get_course_summary(course_id)
    total = int(summary.get('total_enrolled', 0))
    completed = int(summary.get('completed', 0))
    in_progress = int(summary.get('in_progress', 0))
    not_started = int(summary.get('not_started', 0))
    
    return {
        'course_id': course_id,
        'total_enrolled': total,
        'completed': completed,
        'in_progress': in_progress,
        'not_started': not_started,
        'completion_rate': round((completed / total * 100) if total > 0 else 0, 2),
        'started_rate': round(((completed + in_progress) / total * 100) if total > 0 else 0, 2),
        'engagement_rate': round(((total - not_started) / total * 100) if total > 0 else 0, 2),
    }


# =============================================================================
# COURSE BREAKDOWN FUNCTIONS
# =============================================================================

def get_course_breakdown(course_id: int = 9204) -> List[Dict]:
    """
    Get detailed user breakdown for a course.
    
    Args:
        course_id: LearnDash course post ID
    
    Returns:
        List of user dictionaries with progress data
    """
    result = _fetch('course_breakdown', {'course_id': course_id})
    return result.get('data', [])


def filter_users_by_status(status: str, course_id: int = 9204) -> List[Dict]:
    """
    Get users filtered by progress status.
    
    Args:
        status: One of 'Completed', 'In Progress', 'Not Started'
        course_id: LearnDash course post ID
    
    Returns:
        List of users with matching status
    """
    users = get_course_breakdown(course_id)
    return [u for u in users if u.get('progress_status') == status]


def get_completed_users(course_id: int = 9204) -> List[Dict]:
    """Get users who completed the course"""
    return filter_users_by_status('Completed', course_id)


def get_in_progress_users(course_id: int = 9204) -> List[Dict]:
    """Get users currently in progress"""
    return filter_users_by_status('In Progress', course_id)


def get_not_started_users(course_id: int = 9204) -> List[Dict]:
    """Get users who haven't started"""
    return filter_users_by_status('Not Started', course_id)


def get_users_by_email_domain(domain: str, course_id: int = 9204) -> List[Dict]:
    """
    Get users filtered by email domain.
    
    Args:
        domain: Email domain (e.g., 'gmail.com', 'company.com')
        course_id: LearnDash course post ID
    
    Returns:
        List of users with matching email domain
    """
    users = get_course_breakdown(course_id)
    return [u for u in users if (u.get('user_email') or '').endswith('@' + domain)]


def get_recent_completions(limit: int = 10, course_id: int = 9204) -> List[Dict]:
    """
    Get most recent course completions.
    
    Args:
        limit: Number of users to return
        course_id: LearnDash course post ID
    
    Returns:
        List of recently completed users, sorted by completion date
    """
    completed = get_completed_users(course_id)
    
    # Filter out users without completion date
    with_dates = [u for u in completed if u.get('completed_at')]
    
    # Sort by completion date (most recent first)
    sorted_users = sorted(with_dates, key=lambda x: x.get('completed_at', ''), reverse=True)
    
    return sorted_users[:limit]


def get_at_risk_students(days_enrolled: int = 30, course_id: int = 9204) -> List[Dict]:
    """
    Get students who enrolled but haven't started within specified days.
    
    Args:
        days_enrolled: Minimum days since enrollment
        course_id: LearnDash course post ID
    
    Returns:
        List of at-risk students
    """
    from datetime import datetime, timedelta
    
    not_started = get_not_started_users(course_id)
    cutoff_date = datetime.now() - timedelta(days=days_enrolled)
    
    at_risk = []
    for user in not_started:
        enrolled = user.get('enrolled_at')
        if enrolled:
            try:
                enrolled_dt = datetime.strptime(enrolled, '%Y-%m-%d %H:%M:%S')
                if enrolled_dt < cutoff_date:
                    at_risk.append(user)
            except:
                pass
    
    return at_risk


# =============================================================================
# USER LOOKUP FUNCTIONS
# =============================================================================

def find_user_in_course(email: str, course_id: int = 9204) -> Optional[Dict]:
    """
    Find a specific user in a course by email.
    
    Args:
        email: User's email address
        course_id: LearnDash course post ID
    
    Returns:
        User dictionary or None if not found
    """
    users = get_course_breakdown(course_id)
    for user in users:
        if (user.get('user_email') or '').lower() == email.lower():
            return user
    return None


def get_user_progress(email: str, course_id: int = 9204) -> Dict:
    """
    Get progress information for a specific user.
    
    Args:
        email: User's email address
        course_id: LearnDash course post ID
    
    Returns:
        Dictionary with user progress info
    """
    user = find_user_in_course(email, course_id)
    
    if not user:
        return {'found': False, 'email': email}
    
    return {
        'found': True,
        'user_id': user.get('user_id'),
        'name': user.get('display_name'),
        'email': user.get('user_email'),
        'status': user.get('progress_status'),
        'enrolled_at': user.get('enrolled_at'),
        'started_at': user.get('started_at'),
        'completed_at': user.get('completed_at'),
    }


# =============================================================================
# REPORTING FUNCTIONS
# =============================================================================

def generate_course_report(course_id: int = 9204) -> Dict:
    """
    Generate comprehensive course report.
    
    Args:
        course_id: LearnDash course post ID
    
    Returns:
        Complete report dictionary
    """
    summary = get_course_summary(course_id)
    users = get_course_breakdown(course_id)
    
    completed = [u for u in users if u.get('progress_status') == 'Completed']
    in_progress = [u for u in users if u.get('progress_status') == 'In Progress']
    not_started = [u for u in users if u.get('progress_status') == 'Not Started']
    
    total = int(summary.get('total_enrolled', 0))
    
    return {
        'course_id': course_id,
        'summary': {
            'total_enrolled': total,
            'completed': len(completed),
            'in_progress': len(in_progress),
            'not_started': len(not_started),
            'completion_rate': round((len(completed) / total * 100) if total > 0 else 0, 2),
        },
        'users_by_status': {
            'completed': completed,
            'in_progress': in_progress,
            'not_started': not_started,
        },
        'recent_completions': get_recent_completions(5, course_id),
        'at_risk_students': get_at_risk_students(30, course_id),
    }


def export_to_csv(course_id: int = 9204, filename: Optional[str] = None) -> str:
    """
    Export course data to CSV file.
    
    Args:
        course_id: LearnDash course post ID
        filename: Output filename. If None, auto-generates.
    
    Returns:
        Path to created CSV file
    """
    import csv
    from datetime import datetime
    
    if filename is None:
        filename = f"course_{course_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    users = get_course_breakdown(course_id)
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'user_id', 'display_name', 'user_email', 'first_name', 'last_name',
            'progress_status', 'enrolled_at', 'started_at', 'completed_at', 'phone'
        ])
        writer.writeheader()
        
        for user in users:
            writer.writerow({
                'user_id': user.get('user_id'),
                'display_name': user.get('display_name'),
                'user_email': user.get('user_email'),
                'first_name': user.get('first_name'),
                'last_name': user.get('last_name'),
                'progress_status': user.get('progress_status'),
                'enrolled_at': user.get('enrolled_at'),
                'started_at': user.get('started_at'),
                'completed_at': user.get('completed_at'),
                'phone': user.get('phone'),
            })
    
    return filename


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Example usage
    print("=== Webinar Registrations ===")
    registrations = get_webinar_registrations()
    print(f"Total: {len(registrations)}")
    
    print("\n=== SAPA Growth Clinics Registrations ===")
    sapa_regs = get_sapa_growth_clinics_registrations()
    print(f"Total for SAPA Growth Clinics: {len(sapa_regs)}")
    if sapa_regs:
        print(f"First registration: {sapa_regs[0].get('first_name')} {sapa_regs[0].get('last_name')}")
        print(f"Event date: {sapa_regs[0].get('start_date')}")
    
    print("\n=== Registrations by Event ===")
    by_event = count_registrations_by_event()
    for event_id, details in sorted(by_event.items(), key=lambda x: x[1]['count'], reverse=True):
        print(f"- {details['title']}: {details['count']} registrations (Date: {details['start_date']})")
    
    print("\n=== Course Summary ===")
    stats = get_course_stats(9204)
    print(f"Total enrolled: {stats['total_enrolled']}")
    print(f"Completed: {stats['completed']}")
    print(f"In progress: {stats['in_progress']}")
    print(f"Not started: {stats['not_started']}")
    print(f"Completion rate: {stats['completion_rate']}%")
    
    print("\n=== Course Summary ===")
    stats = get_course_stats(8693)
    print(f"Total enrolled: {stats['total_enrolled']}")
    print(f"Completed: {stats['completed']}")
    print(f"In progress: {stats['in_progress']}")
    print(f"Not started: {stats['not_started']}")
    print(f"Completion rate: {stats['completion_rate']}%")
    
    print("\n=== Recent Completions ===")
    recent = get_recent_completions(100, 9204)
    for user in recent:
        print(f"- {user['display_name']} ({user['user_email']}) - {user['completed_at']}")
    
    print("\n=== At Risk Students ===")
    at_risk = get_at_risk_students(100, 9204)
    print(f"Found {len(at_risk)} students enrolled 30+ days but not started")