import React, { useEffect, useState, useRef } from "react";
import axios from "axios";
import {
  Box, Button, Card, CardActionArea, CardContent, Grid, Paper,
  Breadcrumbs, Link, Typography, Tabs, Tab, RadioGroup, FormControlLabel,
  Radio, Stack, CircularProgress, Divider, LinearProgress, Chip,
  IconButton, Fade, Slide, Zoom, Alert, Avatar, Container, Dialog,
  DialogTitle, DialogContent, DialogActions
} from "@mui/material";
import {
  Home as HomeIcon,
  School as SchoolIcon,
  Quiz as QuizIcon,
  MenuBook as BookIcon,
  CheckCircle as CheckIcon,
  Cancel as CancelIcon,
  ArrowForward as ArrowForwardIcon,
  ArrowBack as ArrowBackIcon,
  Download as DownloadIcon,
  Lightbulb as LightbulbIcon,
  Psychology as PsychologyIcon,
  EmojiEvents as TrophyIcon,
  Timeline as TimelineIcon,
  Dashboard as DashboardIcon,
  ExitToApp as ExitToAppIcon,
  AccountBalanceWallet as WalletIcon
} from "@mui/icons-material";

import { marked } from "marked";

function downloadMarkdown(filename, contents) {
  const blob = new Blob([contents], { type: "text/markdown" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

/* === NEW: Compute next unanswered from snapshot attempts === */
function nextCursorFromAttempts(planJson, snapshot) {
  const attempts = snapshot?.attempts || [];
  const byQ = new Map();
  for (const a of attempts) {
    const key = `${(a.subtopic_id || "").toLowerCase()}:${(a.question_id || "").toLowerCase()}`;
    const rec = byQ.get(key) || { count: 0, anyCorrect: false };
    rec.count += 1;
    if (a.correct) rec.anyCorrect = true;
    byQ.set(key, rec);
  }

  for (let i = 0; i < (planJson?.subtopics?.length || 0); i++) {
    const sub = planJson.subtopics[i];
    for (let j = 0; j < (sub?.questions?.length || 0); j++) {
      const q = sub.questions[j];
      const key = `${String(sub.subtopic_id).toLowerCase()}:${String(q.question_id).toLowerCase()}`;
      const rec = byQ.get(key);
      const variants = q.variants || [];
      const totalSlots = 1 + variants.length;
      if (!rec) {
        return { subIdx: i, mcqIdx: j, attemptIdx: 0 };
      }
      if (!rec.anyCorrect && rec.count < totalSlots) {
        return { subIdx: i, mcqIdx: j, attemptIdx: rec.count };
      }
    }
  }
  return null; // everything complete
}

function subtopicCategoryLabel(subtopic) {
  const category = String(subtopic?.category || "").trim();
  const fallback = String(subtopic?.subtopic_title || "").trim();
  return category || fallback || "Uncategorized";
}

function buildCategoryGroups(planJson) {
  const groups = [];
  const byKey = new Map();

  for (const subtopic of planJson?.subtopics || []) {
    const key = subtopicCategoryLabel(subtopic);
    if (!byKey.has(key)) {
      const group = {
        key,
        title: key,
        items: [],
        conceptCount: 0,
        caseCount: 0,
        questionCount: 0,
      };
      byKey.set(key, group);
      groups.push(group);
    }

    const group = byKey.get(key);
    group.items.push(subtopic);
    group.questionCount += subtopic?.questions?.length || 0;
    if (subtopic?.is_case) group.caseCount += 1;
    else group.conceptCount += 1;
  }

  return groups;
}

export default function AdaptiveApp() {
  /* -------- app-level navigation ---------- */
  const [history, setHistory] = useState([]);
  const [view, setView] = useState("home");
  const [supertopics, setSupertopics] = useState([]);
  const [selectedSuper, setSelectedSuper] = useState("");
  const [selectedCategory, setSelectedCategory] = useState("");
  const [topicsList, setTopicsList] = useState([]);

  /* ---------- topic/session state ---------- */
  const [topicId, setTopicId] = useState("");
  const [plan, setPlan] = useState(null);
  const [sessionId, setSessionId] = useState("");

  /* ---------- intra-lesson cursors ---------- */
  const [subIdx, setSubIdx] = useState(0);
  const [mcqIdx, setMcqIdx] = useState(0);
  const [attemptIdx, setAttemptIdx] = useState(0);

  /* ---------- per-question UI ---------- */
  const [choice, setChoice] = useState(null);
  const [result, setResult] = useState(null);
  const [mode, setMode] = useState("question");
  const [tab, setTab] = useState(1);

  // --- NEW state for option-level feedback ---
  const [lastChosenIdx, setLastChosenIdx] = useState(null);   // which option was just attempted
  const [showRationale, setShowRationale] = useState(false);  // show rationale block for attempted option?

  // --- NEW: auto-next-to-variant after incorrect try ---
  const [awaitingAutoNext, setAwaitingAutoNext] = useState(false); // while countdown is active
  const [autoNextIn, setAutoNextIn] = useState(0);                 // seconds remaining
  const autoNextTimeoutRef = useRef(null);
  const autoNextIntervalRef = useRef(null);

  // Use variant-specific correct index when attempts > 0
  const correctIndexForCurrentAttempt = () => {
    const base = currentQ()?.correct_choice_index;
    if (attemptIdx === 0) return base;
    const v = (currentQ()?.variants || []).find(v => v.variant_no === attemptIdx);
    return (v?.correct_choice_index ?? base);
  };

  /* ---------- report ---------- */
  const [finished, setFinished] = useState(false);
  const [reportMd, setReportMd] = useState("");
  const [loadingReport, setLoadingReport] = useState(false);
  const hasActiveSession = Boolean(sessionId) && !finished;

  /* ---------- loading states ---------- */
  const [loading, setLoading] = useState(false);
  const [creditBalance, setCreditBalance] = useState(null);

  /* ---------- resume session state ---------- */
  const [resumeData, setResumeData] = useState([]);
  const [showResumePrompt, setShowResumePrompt] = useState(false);
  const [activeSessionPromptOpen, setActiveSessionPromptOpen] = useState(false);
  const [pendingLockedView, setPendingLockedView] = useState("home");
  const [terminatePrompt, setTerminatePrompt] = useState({ open: false, session: null });
  const [terminatingSession, setTerminatingSession] = useState(false);

  /* ---------- dashboard state ---------- */
  const [dashboardRows, setDashboardRows] = useState([]);
  const [returnUrlGet, setReturnUrlGet] = useState("");
  const [returningToPlatform, setReturningToPlatform] = useState(false);

  /* ---------- NEW: lock state ---------- */
  const [locked, setLocked] = useState(false);

  // Read user_id from query string: ?user_id=<uuid-from-launch-endpoint>
  const params = new URLSearchParams(window.location.search);
  const USER_ID = params.get("user_id") || ""; // or keep a dev fallback if you want

  if (!USER_ID) {
    console.error(
      "Missing user_id in URL. " +
      "Make sure the Education Platform redirects with ?user_id=<adaptive-user-uuid>."
    );
  }

  useEffect(() => {
    if (!USER_ID) {
      setReturnUrlGet("");
      return;
    }

    let cancelled = false;
    axios.get(`/api/api/users/${USER_ID}/return-url`)
      .then(r => {
        if (!cancelled) setReturnUrlGet(r.data?.return_url_get || "");
      })
      .catch(() => {
        if (!cancelled) setReturnUrlGet("");
      });

    return () => {
      cancelled = true;
    };
  }, [USER_ID]);

  const refreshCreditBalance = async () => {
    if (!USER_ID) {
      setCreditBalance(null);
      return null;
    }

    try {
      const { data } = await axios.get(`/api/api/users/${USER_ID}/credits`);
      const nextBalance = Number(data?.credit_balance ?? 0);
      setCreditBalance(Number.isFinite(nextBalance) ? nextBalance : 0);
      return nextBalance;
    } catch (error) {
      console.error("Error loading credit balance:", error);
      setCreditBalance(null);
      return null;
    }
  };

  useEffect(() => {
    refreshCreditBalance();
  }, [USER_ID]);


  /* ---------- bootstrap: load supertopics ---------- */
  useEffect(() => {
    setLoading(true);
    axios.get("/api/api/supertopics")
      .then(r => {
        setSupertopics(r.data || []);
        setLoading(false);
      })
      .catch(() => {
        setSupertopics([]);
        setLoading(false);
      });
  }, []);

  /* ---------- check for unfinished sessions on app load ---------- */
  useEffect(() => {
    axios.get(`/api/api/resume-status/${USER_ID}`)
      .then(r => {
        const u = r.data?.unfinished || [];
        if (u.length > 0) {
          const hasInvalidData = u.some(session => !session.topic_id || !session.topic_name);
          if (hasInvalidData) {
            // Reload the site if there's invalid data
            window.location.reload();
            return;
          }
          setResumeData(u);
          setShowResumePrompt(true);
        }
      })
      .catch(() => {});
  }, []);

  /* ---------- NEW: derive lock from local state ---------- */
  useEffect(() => {
    setLocked(hasActiveSession || (resumeData && resumeData.length > 0));
  }, [hasActiveSession, resumeData]);

  /* ---------- NEW: optional robust lock polling ---------- */
  useEffect(() => {
    const poll = async () => {
      try {
        const { data } = await axios.get(`/api/api/lock-status/${USER_ID}`);
        if (data) {
          setLocked(Boolean(data.locked) || hasActiveSession || (resumeData && resumeData.length > 0));
        }
      } catch {}
    };
    const id = setInterval(poll, 15000);
    poll();
    return () => clearInterval(id);
  }, [USER_ID, hasActiveSession, resumeData]);

  /* ---------- NEW: block tab/window reload while locked ---------- */
  useEffect(() => {
    const onBeforeUnload = (e) => {
      if (locked) {
        e.preventDefault();
        e.returnValue = "";
      }
    };
    window.addEventListener("beforeunload", onBeforeUnload);
    return () => window.removeEventListener("beforeunload", onBeforeUnload);
  }, [locked]);

  /* ---------- idle timer: 5 minutes inactivity ---------- */
  useEffect(() => {
    if (!sessionId || !topicId || !plan) return;

    let timer;
    const reset = () => {
      clearTimeout(timer);
      timer = setTimeout(async () => {
        alert("Session progress is being saved because of inactivity.");

        try {
          await axios.post("/api/api/session/idle-save", {
            user_id: USER_ID,
            topic_id: topicId,
            session_id: sessionId,
            cursors: { subIdx, mcqIdx, attemptIdx, view, tab, history }
          });
        } catch (e) {
          console.error("Error saving idle session:", e);
        }

        setPlan(null);
        setSessionId("");
        setHistory([]);
        setView("home");
      }, 5 * 60 * 1000);
    };

    const events = ["mousemove", "keydown", "click", "touchstart"];
    events.forEach(e => window.addEventListener(e, reset));
    reset();

    return () => {
      clearTimeout(timer);
      events.forEach(e => window.removeEventListener(e, reset));
    };
  }, [sessionId, topicId, plan, subIdx, mcqIdx, attemptIdx, view, tab, history]);

  /* ---------- NEW: continuous non-destructive snapshots ---------- */
  useEffect(() => {
    if (!sessionId || !topicId || !plan) return;
    const save = async () => {
      try {
        await axios.post("/api/api/session/snapshot", {
          user_id: USER_ID,
          topic_id: topicId,
          session_id: sessionId,
          cursors: { subIdx, mcqIdx, attemptIdx, view, tab, history }
        });
      } catch (e) {
        console.warn("Snapshot save failed", e);
      }
    };
    const id = setTimeout(save, 400);
    return () => clearTimeout(id);
  }, [sessionId, topicId, plan, subIdx, mcqIdx, attemptIdx, view, tab, history]);

  /* ---------- report generation on finish ---------- */
  useEffect(() => {
    if (!finished || !sessionId) return;
    setLoadingReport(true);
    axios.get(`/api/api/report/${sessionId}`)
      .then(r => setReportMd(r.data.markdown))
      .catch(() => setReportMd("**Error:** unable to generate report."))
      .finally(() => {
        setLoadingReport(false);
        refreshCreditBalance();
      });
  }, [finished, sessionId]);

  useEffect(() => {
    if (!plan) {
      setSelectedCategory("");
      return;
    }

    const groups = buildCategoryGroups(plan);
    if (groups.length === 0) {
      setSelectedCategory("");
      return;
    }

    if (!groups.some(group => group.key === selectedCategory)) {
      setSelectedCategory(groups[0].key);
    }
  }, [plan, selectedCategory]);

  /* ---------- helpers ---------- */
  const currentSub = () => plan?.subtopics?.[subIdx];
  const categoryGroups = () => buildCategoryGroups(plan);
  const currentCategoryGroup = () =>
    categoryGroups().find(group => group.key === selectedCategory) || categoryGroups()[0] || null;
  const currentCategoryItems = () => currentCategoryGroup()?.items || [];
  const currentQ = () => currentSub()?.questions?.[mcqIdx];
  const variants = () => currentQ()?.variants || [];
  const currentStem = () =>
    attemptIdx === 0 ? currentQ()?.stem : (variants()[attemptIdx - 1]?.stem ?? currentQ()?.stem);
  // Prefer a backend-sent boolean (is_case). Fallback to title heuristics so UI never crashes.
  const isCase =
    Boolean(currentSub()?.is_case) ||
    /^case( study)?:/i.test(String(currentSub()?.subtopic_title || ''));
  const getProgress = () => {
    if (!plan) return 0;
    const totalQuestions = plan.subtopics.reduce((sum, sub) => sum + sub.questions.length, 0);
    const currentQuestionNumber = plan.subtopics.slice(0, subIdx).reduce((sum, sub) => sum + sub.questions.length, 0) + mcqIdx + 1;
    return (currentQuestionNumber / totalQuestions) * 100;
  };

  /* ---------- NEW: enforce lock on navigations ---------- */
  const enforceLock = (desiredView) => {
    const allowed = ["categories", "concept", "questions"];
    if (!allowed.includes(desiredView) && hasActiveSession) {
      setPendingLockedView(desiredView);
      setActiveSessionPromptOpen(true);
      if (plan) setView(allowed.includes(view) ? view : "questions");
      return true; // blocked
    }
    if (!allowed.includes(desiredView) && resumeData && resumeData.length > 0) {
      setShowResumePrompt(true);
      if (plan) setView(allowed.includes(view) ? view : "questions");
      return true; // blocked
    }
    return false;
  };

  const chooseSuper = async (name) => {
    if (enforceLock("topics")) return;
    setLoading(true);
    try {
      setSelectedSuper(name);
      const { data } = await axios.get("/api/api/topics", {params: { supertopic: name, user_id: USER_ID },});

      setTopicsList(data || []);
      setView("topics");
    } catch (error) {
      console.error("Error loading topics:", error);
      setTopicsList([]);
    } finally {
      setLoading(false);
    }
  };

  const startTopic = async (tId) => {
    if (enforceLock("categories")) return;
    setLoading(true);
    try {
      const { data: planJson } = await axios.get(`/api/api/lesson/${tId}/${USER_ID}`);
      const groups = buildCategoryGroups(planJson);
      setHistory([]);
      setSessionId("");
      setTopicId(tId);
      setPlan(planJson);
      setSelectedSuper(planJson?.supertopic || selectedSuper || "");
      setSelectedCategory(groups[0]?.key || "");

      setSubIdx(0); setMcqIdx(0); setAttemptIdx(0);
      setChoice(null); setResult(null); setMode("question"); setTab(0);
      setFinished(false); setReportMd(""); setLoadingReport(false);

      setView("categories");
    } catch (error) {
      console.error("Error starting topic:", error);
    } finally {
      setLoading(false);
    }
  };

  /* ---------- UPDATED: resume to next unanswered ---------- */
  const resumeSession = async (t) => {
    setLoading(true);
    try {
      const { data: snapshot } = await axios.get(`/api/api/resume/${USER_ID}/${t.topic_id}`);
      const { data: planJson } = await axios.get(`/api/api/lesson/${t.topic_id}/${USER_ID}`);
      const groups = buildCategoryGroups(planJson);

      setPlan(planJson);
      setTopicId(t.topic_id);
      setSessionId(snapshot.session_id);
      setSelectedSuper(planJson?.supertopic || selectedSuper || "");

      const next = nextCursorFromAttempts(planJson, snapshot);
      if (next) {
        setSubIdx(next.subIdx);
        setMcqIdx(next.mcqIdx);
        setAttemptIdx(next.attemptIdx);
        setSelectedCategory(subtopicCategoryLabel(planJson.subtopics?.[next.subIdx]));
        setMode("question");
        setTab(1);
        setView("questions");
      } else {
        setSelectedCategory(groups[0]?.key || "");
        setFinished(true);
        setView("questions");
      }

      const c = snapshot.cursors || {};
      setHistory(c.history || []);
      setShowResumePrompt(false);
    } catch (error) {
      console.error("Error resuming session:", error);
    } finally {
      setLoading(false);
    }
  };

  const openTerminatePrompt = (session) => {
    setTerminatePrompt({ open: true, session });
  };

  const closeTerminatePrompt = () => {
    if (!terminatingSession) {
      setTerminatePrompt({ open: false, session: null });
    }
  };

  const terminateSession = async () => {
    const t = terminatePrompt.session;
    if (!t) return;

    setTerminatingSession(true);
    try {
      const { data } = await axios.delete(`/api/api/resume/${USER_ID}/${t.topic_id}`);
      const updated = resumeData.filter(x => x.topic_id !== t.topic_id);
      setResumeData(updated);
      if (Number.isFinite(Number(data?.credit_balance))) {
        setCreditBalance(Number(data.credit_balance));
      } else {
        refreshCreditBalance();
      }
      if (updated.length === 0) {
        setShowResumePrompt(false);
        setView("home");
      }
    } catch (error) {
      console.error("Error terminating session:", error);
      alert("Unable to terminate this session. Please try again so your credit balance can be updated correctly.");
    } finally {
      setTerminatingSession(false);
      setTerminatePrompt({ open: false, session: null });
    }
  };

  const openCategory = (categoryKey) => {
    setSelectedCategory(categoryKey);
    setView("concept");
  };

  const openCurrentCategory = () => {
    const subtopic = currentSub();
    if (!subtopic) return;
    setSelectedCategory(subtopicCategoryLabel(subtopic));
    setView("concept");
  };

  const beginQuestions = async () => {
    try {
      if (!hasActiveSession) {
        setLoading(true);
        const { data: sessId } = await axios.post("/api/api/session", { user_id: USER_ID, topic_id: topicId });
        setSessionId(sessId);
        setFinished(false);
        setReportMd("");
        setLoadingReport(false);
        setHistory([]);
        setSubIdx(0);
        setMcqIdx(0);
        setAttemptIdx(0);
        setChoice(null);
        setResult(null);
        setShowRationale(false);
        setLastChosenIdx(null);
        setAwaitingAutoNext(false);
        setAutoNextIn(0);
        clearAutoNextTimers();
        setMode("question");
      }
      setView("questions");
      setTab(1);
    } catch (error) {
      console.error("Error starting session:", error);
    } finally {
      setLoading(false);
    }
  };

  const goHome = async () => {
  if (enforceLock("home")) return;

  // First reset the view state
  setView("home");
  setSelectedSuper("");
  setSelectedCategory("");
  setTopicsList([]);
  setPlan(null);
  setTopicId("");
  setSessionId("");

  // Refetch unfinished sessions from backend when going home
  try {
    const { data } = await axios.get(`/api/api/resume-status/${USER_ID}`);
    const u = data?.unfinished || [];

    if (u.length > 0) {
      // Check if any session has missing topic_id or topic_name
      const hasInvalidData = u.some(session => !session.topic_id || !session.topic_name);
      if (hasInvalidData) {
        // Reload the site if there's invalid data
        window.location.reload();
        return;
      }
      setResumeData(u);
      setShowResumePrompt(true);
    } else {
      setResumeData([]);
      setShowResumePrompt(false);
    }
  } catch (error) {
    console.error("Error fetching resume status:", error);
    setResumeData([]);
    setShowResumePrompt(false);
  }
};

  const returnToEducationPlatform = async () => {
    if (!USER_ID) return;

    setReturningToPlatform(true);
    try {
      const { data } = await axios.get(`/api/api/users/${USER_ID}/return-url`);
      const redirectUrl = data?.return_url_get || returnUrlGet;

      if (!redirectUrl) {
        alert("Return URL is not available for this user.");
        return;
      }

      setReturnUrlGet(redirectUrl);
      window.location.assign(redirectUrl);
    } catch (error) {
      console.error("Error loading return URL:", error);
      alert("Unable to return to the Education Platform. Please try again.");
    } finally {
      setReturningToPlatform(false);
    }
  };

  const loadDashboard = async () => {
  if (enforceLock("dashboard")) return;

  // Check for unfinished sessions before loading dashboard
  try {
    const { data: resumeStatusData } = await axios.get(`/api/api/resume-status/${USER_ID}`);
    const u = resumeStatusData?.unfinished || [];

    if (u.length > 0) {
      // Check if any session has missing topic_id or topic_name
      const hasInvalidData = u.some(session => !session.topic_id || !session.topic_name);
      if (hasInvalidData) {
        // Reload the site if there's invalid data
        window.location.reload();
        return;
      }
      setResumeData(u);
      setShowResumePrompt(true);
      return; // Don't proceed to dashboard, show resume prompt instead
    }

    // If no unfinished sessions, proceed to load dashboard
    const { data } = await axios.get(`/api/api/dashboard/${USER_ID}`);
    setDashboardRows(data.sessions || []);
    setView("dashboard");
  } catch (error) {
    console.error("Error loading dashboard:", error);
  }
};

  const clearLessonState = () => {
    clearAutoNextTimers();
    setPlan(null);
    setTopicId("");
    setSessionId("");
    setSelectedCategory("");
    setSubIdx(0);
    setMcqIdx(0);
    setAttemptIdx(0);
    setChoice(null);
    setResult(null);
    setMode("question");
    setTab(1);
    setLastChosenIdx(null);
    setShowRationale(false);
    setAwaitingAutoNext(false);
    setAutoNextIn(0);
    setFinished(false);
    setReportMd("");
    setLoadingReport(false);
    setHistory([]);
  };

  const saveAndLeaveActiveSession = async () => {
    if (!hasActiveSession || !topicId || !sessionId) {
      setActiveSessionPromptOpen(false);
      return;
    }

    setLoading(true);
    try {
      await axios.post("/api/api/session/idle-save", {
        user_id: USER_ID,
        topic_id: topicId,
        session_id: sessionId,
        cursors: { subIdx, mcqIdx, attemptIdx, view, tab, history }
      });

      const { data: resumeStatusData } = await axios.get(`/api/api/resume-status/${USER_ID}`);
      const unfinished = resumeStatusData?.unfinished || [];
      setResumeData(unfinished);
      setShowResumePrompt(false);

      const targetView = pendingLockedView;
      clearLessonState();

      if (targetView === "dashboard") {
        const { data } = await axios.get(`/api/api/dashboard/${USER_ID}`);
        setDashboardRows(data.sessions || []);
        setView("dashboard");
      } else if (targetView === "topics") {
        setView("topics");
      } else {
        setSelectedSuper("");
        setTopicsList([]);
        setDashboardRows([]);
        setView("home");
      }
    } catch (error) {
      console.error("Error saving current session before leaving:", error);
    } finally {
      setActiveSessionPromptOpen(false);
      setLoading(false);
    }
  };

  const clearAutoNextTimers = () => {
    if (autoNextTimeoutRef.current) {
      clearTimeout(autoNextTimeoutRef.current);
      autoNextTimeoutRef.current = null;
    }
    if (autoNextIntervalRef.current) {
      clearInterval(autoNextIntervalRef.current);
      autoNextIntervalRef.current = null;
    }
  };

  // === UPDATED submit(): do not auto-advance on incorrect; show rationale immediately; use variant-specific correct index
  // Additionally: when incorrect and a variant remains -> disable answering and auto-redirect to "Next Try" in 5s
  const submit = async () => {
    const chosenIdx = parseInt(choice, 10);
    const correctIdx = correctIndexForCurrentAttempt();
    const isCorrect = chosenIdx === correctIdx;

    try {
      await axios.post("/api/api/answer", {
        session_id: sessionId,
        topic_id: topicId,
        subtopic_id: currentSub().subtopic_id,
        question_id: currentQ().question_id,
        variant_no: attemptIdx,
        chosen_index: chosenIdx,
      });

      // keep local history (used for final review coloring)
      setHistory(h => [...h, {
        subtopic_id: currentSub().subtopic_id,
        question_id: currentQ().question_id,
        variant_no: attemptIdx,
        chosen_index: chosenIdx,
        correct: isCorrect
      }]);

      setLastChosenIdx(chosenIdx);
      setShowRationale(true);
      setResult(isCorrect ? "correct" : "incorrect");

      const haveMoreVariants = attemptIdx < (currentQ()?.variants?.length || 0);

      if (isCorrect || !haveMoreVariants) {
        // Go to explanation immediately
        clearAutoNextTimers();
        setAwaitingAutoNext(false);
        setAutoNextIn(0);
        setMode("explain");
      } else {
        // Incorrect and variants remain:
        // Disable answering and start a 5s countdown to auto Next Try
        setAwaitingAutoNext(true);
        setAutoNextIn(5);
        clearAutoNextTimers();
        autoNextIntervalRef.current = setInterval(() => {
          setAutoNextIn(prev => {
            if (prev <= 1) return 0;
            return prev - 1;
          });
        }, 1000);
        autoNextTimeoutRef.current = setTimeout(() => {
          nextTry();
        }, 5000);
      }
    } catch (error) {
      console.error("Error submitting answer:", error);
    }
  };

  // === NEW: Next Try handler ===
  const nextTry = () => {
    clearAutoNextTimers();
    setAwaitingAutoNext(false);
    setAutoNextIn(0);
    setAttemptIdx(attemptIdx + 1);
    setChoice(null);
    setResult(null);
    setShowRationale(false);
    setLastChosenIdx(null);
  };

  const proceed = () => {
    clearAutoNextTimers();
    setAwaitingAutoNext(false);
    setAutoNextIn(0);
    const lastSub = plan.subtopics.length - 1;
    const lastMcq = currentSub().questions.length - 1;
    if (subIdx === lastSub && mcqIdx === lastMcq) {
      setFinished(true);
      return;
    }
    if (mcqIdx < lastMcq) setMcqIdx(mcqIdx + 1);
    else { setSubIdx(subIdx + 1); setMcqIdx(0); }
    setAttemptIdx(0); setChoice(null); setResult(null); setShowRationale(false); setLastChosenIdx(null); setMode("question"); setTab(1);
  };

  // Cleanup timers on unmount
  useEffect(() => {
    return () => clearAutoNextTimers();
  }, []);

  const formatCredits = (value) => {
    const credits = Number(value);
    const safeCredits = Number.isFinite(credits) ? credits : 1;
    return `${safeCredits} credit${safeCredits === 1 ? "" : "s"}`;
  };

  const topicCost = (topic) => {
    const credits = Number(topic?.credits);
    return Number.isFinite(credits) ? credits : 1;
  };

  const renderCreditBalance = () => (
    <Chip
      icon={<WalletIcon />}
      label={
        creditBalance === null
          ? "Credits available: --"
          : `${formatCredits(creditBalance)} available`
      }
      sx={{
        background: 'white',
        color: '#2c3e50',
        border: '1px solid rgba(255, 107, 53, 0.35)',
        boxShadow: '0 4px 12px rgba(44, 62, 80, 0.08)',
        fontWeight: 'bold',
        '& .MuiChip-icon': {
          color: '#ff6b35'
        }
      }}
    />
  );

  const renderTopCreditBar = (sx = {}) => (
    <Stack direction="row" justifyContent="flex-end" sx={{ mb: 2, ...sx }}>
      {renderCreditBalance()}
    </Stack>
  );

  const dashboardStatus = (status) => {
    const normalized = String(status || "").toLowerCase();
    if (normalized === "completed") {
      return {
        label: "Completed",
        background: 'linear-gradient(135deg, #2ed573 0%, #1dd1a1 100%)',
      };
    }
    if (normalized === "terminated") {
      return {
        label: "Terminated",
        background: 'linear-gradient(135deg, #e74c3c 0%, #c0392b 100%)',
      };
    }
    return {
      label: "In Progress",
      background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
    };
  };

  const renderLessonFooter = (backLabel, onBack) => (
    <Paper
      elevation={8}
      sx={{
        position: 'fixed',
        bottom: 0,
        left: 0,
        right: 0,
        background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.95) 0%, rgba(248, 250, 252, 0.95) 100%)',
        backdropFilter: 'blur(20px)',
        borderTop: '1px solid rgba(195, 207, 226, 0.3)',
        zIndex: 1000,
        py: 0.75,
        px: 3,
      }}
    >
      <Container maxWidth="lg">
        <Stack
          direction="row"
          spacing={3}
          sx={{
            justifyContent: 'center',
            alignItems: 'center'
          }}
        >
          <Button
            variant="outlined"
            onClick={onBack}
            startIcon={<ArrowBackIcon />}
            sx={{
              borderRadius: 25,
              px: 2.5,
              py: 0.5,
              borderColor: '#3498db',
              color: '#3498db',
              fontWeight: 'bold',
              borderWidth: '2px',
              textTransform: 'none',
              fontSize: '1rem',
              transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
              '&:hover': {
                borderColor: '#3498db',
                background: 'rgba(52, 152, 219, 0.1)',
                borderWidth: '2px',
                transform: 'translateY(-2px)',
                boxShadow: '0 8px 25px rgba(52, 152, 219, 0.2)'
              }
            }}
          >
            {backLabel}
          </Button>

          <Box sx={{
            height: 20,
            width: 1,
            background: 'linear-gradient(to bottom, transparent, rgba(195, 207, 226, 0.5), transparent)',
            mx: 0.5
          }} />

          <Button
            variant="contained"
            onClick={beginQuestions}
            endIcon={<QuizIcon />}
            sx={{
              borderRadius: 25,
              px: 3,
              py: 0.5,
              background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
              boxShadow: '0 8px 25px rgba(255, 107, 53, 0.3)',
              fontWeight: 'bold',
              textTransform: 'none',
              fontSize: '1.1rem',
              border: '2px solid transparent',
              transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
              '&:hover': {
                boxShadow: '0 12px 35px rgba(255, 107, 53, 0.4)',
                background: 'linear-gradient(135deg, #f7931e 0%, #ff6b35 100%)',
                transform: 'translateY(-3px)'
              }
            }}
          >
            {hasActiveSession ? "Continue Session" : "Start Session"}
          </Button>
        </Stack>
      </Container>
    </Paper>
  );

  const activeSessionDestinationLabel = {
    home: "Home",
    dashboard: "Dashboard",
    topics: "Topics",
  }[pendingLockedView] || "that page";

  const activeSessionDialog = (
    <Dialog
      open={activeSessionPromptOpen}
      onClose={() => setActiveSessionPromptOpen(false)}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
        Session In Progress
      </DialogTitle>
      <DialogContent>
        <Typography sx={{ color: '#34495e', lineHeight: 1.7 }}>
          You already have a session in progress. You can keep going where you are, or we can save your progress and take you to {activeSessionDestinationLabel}.
        </Typography>
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button
          onClick={() => setActiveSessionPromptOpen(false)}
          variant="outlined"
          sx={{
            borderRadius: 2,
            textTransform: 'none',
            fontWeight: 'bold'
          }}
        >
          Continue Session
        </Button>
        <Button
          onClick={saveAndLeaveActiveSession}
          variant="contained"
          sx={{
            borderRadius: 2,
            textTransform: 'none',
            fontWeight: 'bold',
            background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)'
          }}
        >
          Save and Go to {activeSessionDestinationLabel}
        </Button>
      </DialogActions>
    </Dialog>
  );

  const terminateSessionDialog = (
    <Dialog
      open={terminatePrompt.open}
      onClose={closeTerminatePrompt}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
        Terminate Session?
      </DialogTitle>
      <DialogContent>
        <Stack spacing={2}>
          <Typography sx={{ color: '#34495e', lineHeight: 1.7 }}>
            Are you sure you want to terminate this session? The attempt credits for this session will be lost.
          </Typography>
          <Alert severity="warning">
            Save your progress if you want to preserve your credits and continue this session later.
          </Alert>
        </Stack>
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button
          onClick={closeTerminatePrompt}
          variant="outlined"
          disabled={terminatingSession}
          sx={{
            borderRadius: 2,
            textTransform: 'none',
            fontWeight: 'bold'
          }}
        >
          Save Progress
        </Button>
        <Button
          onClick={terminateSession}
          variant="contained"
          color="error"
          disabled={terminatingSession}
          sx={{
            borderRadius: 2,
            textTransform: 'none',
            fontWeight: 'bold'
          }}
        >
          {terminatingSession ? "Terminating..." : "Terminate Session"}
        </Button>
      </DialogActions>
    </Dialog>
  );

  /* ---------- LOADING SCREEN ---------- */
  if (loading) {
    return (
      <Box sx={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        position: 'relative',
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)'
      }}>
        <Box sx={{ position: 'absolute', top: 24, right: 24 }}>
          {renderCreditBalance()}
        </Box>
        <Stack alignItems="center" spacing={3}>
          <CircularProgress size={60} sx={{ color: '#ff6b35' }} />
          <Typography variant="h6" sx={{ color: '#2c3e50', fontWeight: 'bold' }}>Loading...</Typography>
        </Stack>
      </Box>
    );
  }

  /* ---------- RESUME PROMPT VIEW ---------- */
  if (showResumePrompt && resumeData.length > 0) {
    return (
      <Box sx={{
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
        py: 4
      }}>
        <Container maxWidth="md">
          {renderTopCreditBar()}
          <Fade in timeout={800}>
            <Paper sx={{
              p: 4,
              borderRadius: 4,
              boxShadow: '0 16px 32px rgba(44, 62, 80, 0.1)',
              background: 'white',
              border: '1px solid rgba(195, 207, 226, 0.3)'
            }}>
              <Stack alignItems="center" spacing={3} sx={{ mb: 4 }}>
                <TimelineIcon sx={{ fontSize: 48, color: '#ff6b35' }} />
                <Typography variant="h4" sx={{ color: '#2c3e50', fontWeight: 'bold' }}>
                  Unfinished Sessions
                </Typography>
              </Stack>

              <Typography variant="body1" sx={{ mb: 3, color: '#34495e', textAlign: 'center' }}>
                You have unfinished learning sessions. Would you like to continue or start fresh?
              </Typography>

              <Stack spacing={2}>
                {resumeData.map((t) => (
                  <Paper
                    key={t.topic_id}
                    sx={{
                      p: 3,
                      borderRadius: 2,
                      background: 'linear-gradient(135deg, rgba(255, 107, 53, 0.05) 0%, rgba(247, 147, 30, 0.05) 100%)',
                      border: '1px solid rgba(255, 107, 53, 0.2)',
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center'
                    }}
                  >
                    <Box>
                      <Typography variant="h6" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                        {t.topic_name}
                      </Typography>
                      <Typography variant="body2" sx={{ color: '#34495e', mt: 0.5 }}>
                        Topic ID: {t.topic_id}
                      </Typography>
                    </Box>
                    <Stack direction="row" spacing={2}>
                      <Button
                        variant="contained"
                        onClick={() => resumeSession(t)}
                        sx={{
                          background: 'linear-gradient(135deg, #2ed573 0%, #1dd1a1 100%)',
                          boxShadow: '0 4px 12px rgba(46, 213, 115, 0.3)',
                          fontWeight: 'bold',
                          textTransform: 'none'
                        }}
                      >
                        Continue Session
                      </Button>
                      <Button
                        variant="outlined"
                        color="error"
                        onClick={() => openTerminatePrompt(t)}
                        sx={{
                          fontWeight: 'bold',
                          textTransform: 'none'
                        }}
                      >
                        Terminate
                      </Button>
                    </Stack>
                  </Paper>
                ))}
              </Stack>

              <Button
                fullWidth
                variant="outlined"
                onClick={() => {
                  setShowResumePrompt(false);
                  setView("home");
                }}
                sx={{ mt: 3, fontWeight: 'bold', textTransform: 'none' }}
              >
                Back to Home
              </Button>
            </Paper>
          </Fade>
          {terminateSessionDialog}
        </Container>
      </Box>
    );
  }

  /* ---------- HOME: SuperTopics grid ---------- */
  if (view === "home") {
    return (
      <Box sx={{
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
        py: 4
      }}>
        <Container maxWidth="lg">
          <Fade in timeout={800}>
            <Box>
              <Stack
                direction={{ xs: "column", md: "row" }}
                alignItems={{ xs: "stretch", md: "center" }}
                justifyContent="space-between"
                spacing={2}
                sx={{ mb: 4 }}
              >
                <Stack direction="row" alignItems="center" spacing={2}>
                  <PsychologyIcon sx={{ fontSize: 48, color: '#ff6b35' }} />
                  <Typography variant="h3" sx={{ color: '#2c3e50', fontWeight: 'bold' }}>
                    Your Pediatrics Tutor
                  </Typography>
                </Stack>
                <Stack
                  direction={{ xs: "column", sm: "row" }}
                  spacing={1.5}
                  sx={{ width: { xs: "100%", md: "auto" } }}
                >
                  {renderCreditBalance()}
                  <Button
                    variant="outlined"
                    startIcon={<ExitToAppIcon />}
                    onClick={returnToEducationPlatform}
                    disabled={!USER_ID || returningToPlatform}
                    sx={{
                      borderRadius: 25,
                      px: 2.5,
                      py: 0.5,
                      borderColor: '#2c3e50',
                      color: '#2c3e50',
                      fontWeight: 'bold',
                      textTransform: 'none',
                      fontSize: '1rem',
                      width: { xs: "100%", sm: "auto" },
                      '&:hover': {
                        borderColor: '#ff6b35',
                        color: '#ff6b35',
                        background: 'rgba(255, 107, 53, 0.08)'
                      }
                    }}
                  >
                    {returningToPlatform ? "Returning..." : "Return to Platform"}
                  </Button>
                  <Button
                    variant="outlined"
                    startIcon={<DashboardIcon />}
                    onClick={loadDashboard}
                    sx={{
                      borderRadius: 25,
                      px: 2.5,
                      py: 0.5,
                      borderColor: '#3498db',
                      color: '#3498db',
                      fontWeight: 'bold',
                      textTransform: 'none',
                      fontSize: '1rem',
                      width: { xs: "100%", sm: "auto" }
                    }}
                  >
                    Dashboard
                  </Button>
                </Stack>
              </Stack>

              <Typography variant="h5" sx={{ mb: 4, color: '#34495e', textAlign: 'center', fontWeight: '500' }}>
                Choose your learning journey
              </Typography>

              <Grid container spacing={3}>
                {supertopics.map((s, index) => (
                  <Grid item xs={12} sm={6} md={4} key={s}>
                    <Zoom in timeout={800 + index * 100}>
                      <Card sx={{
                        borderRadius: 4,
                        boxShadow: '0 12px 24px rgba(44, 62, 80, 0.1)',
                        background: 'white',
                        border: '1px solid rgba(195, 207, 226, 0.3)',
                        transition: 'all 0.3s ease',
                        '&:hover': {
                          transform: 'translateY(-8px)',
                          boxShadow: '0 20px 40px rgba(255, 107, 53, 0.15)',
                          border: '1px solid rgba(255, 107, 53, 0.3)'
                        }
                      }}>
                        <CardActionArea onClick={() => chooseSuper(s)} sx={{ p: 3 }}>
                          <Stack alignItems="center" spacing={2}>
                            <Avatar sx={{
                              width: 64,
                              height: 64,
                              background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
                              fontSize: 32,
                              boxShadow: '0 8px 16px rgba(255, 107, 53, 0.3)'
                            }}>
                              <SchoolIcon />
                            </Avatar>
                            <Typography variant="h6" sx={{ fontWeight: 'bold', textAlign: 'center', color: '#2c3e50' }}>
                              {s}
                            </Typography>
                            <Typography variant="body2" sx={{ color: '#34495e', textAlign: 'center', opacity: 0.8 }}>
                              Explore topics and enhance your knowledge
                            </Typography>
                          </Stack>
                        </CardActionArea>
                      </Card>
                    </Zoom>
                  </Grid>
                ))}
              </Grid>
            </Box>
          </Fade>
        </Container>
      </Box>
    );
  }

  /* ---------- TOPICS under selected SuperTopic ---------- */
  if (view === "topics") {
    return (
      <Box sx={{
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
        py: 4
      }}>
        <Container maxWidth="lg">
          {renderTopCreditBar()}
          <Slide direction="right" in timeout={600}>
            <Box>
              <Paper sx={{
                p: 2,
                mb: 3,
                borderRadius: 3,
                background: 'white',
                border: '1px solid rgba(195, 207, 226, 0.3)',
                boxShadow: '0 4px 12px rgba(44, 62, 80, 0.08)'
              }}>
                <Breadcrumbs>
                  <Link
                    underline="hover"
                    onClick={goHome}
                    sx={{
                      cursor: "pointer",
                      display: 'flex',
                      alignItems: 'center',
                      gap: 1,
                      color: '#3498db',
                      '&:hover': { color: '#ff6b35' }
                    }}
                  >
                    <HomeIcon fontSize="small" />
                    Home
                  </Link>
                  <Typography color="#2c3e50" sx={{ fontWeight: 'bold' }}>
                    {selectedSuper}
                  </Typography>
                </Breadcrumbs>
              </Paper>

              <Typography variant="h4" sx={{ mb: 4, color: '#2c3e50', fontWeight: 'bold' }}>
                {selectedSuper} Topics
              </Typography>

              <Grid container spacing={3}>
                {topicsList.map((t, index) => (
                  <Grid item xs={12} sm={6} md={4} key={t.topic_id}>
                    <Zoom in timeout={600 + index * 100}>
                      <Card sx={{
                        borderRadius: 4,
                        boxShadow: '0 12px 24px rgba(44, 62, 80, 0.1)',
                        background: 'white',
                        border: '1px solid rgba(195, 207, 226, 0.3)',
                        height: "100%",
                        transition: 'all 0.3s ease',
                        '&:hover': {
                          transform: 'translateY(-8px)',
                          boxShadow: '0 20px 40px rgba(255, 107, 53, 0.15)',
                          border: '1px solid rgba(255, 107, 53, 0.3)'
                        }
                      }}>
                        <CardActionArea onClick={() => startTopic(t.topic_id)} sx={{ height: "100%", p: 3 }}>
                          <Stack alignItems="center" spacing={2}>
                            <Avatar sx={{
                              width: 56,
                              height: 56,
                              background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
                              fontSize: 24,
                              boxShadow: '0 6px 12px rgba(255, 107, 53, 0.3)'
                            }}>
                              <BookIcon />
                            </Avatar>
                            <Typography variant="h6" sx={{ fontWeight: 'bold', textAlign: 'center', color: '#2c3e50' }}>
                              {t.topic_name}
                            </Typography>
                            <Typography variant="body2" sx={{ color: '#34495e', textAlign: 'center', opacity: 0.8 }}>
                              Start with concept overview, then practice with questions
                            </Typography>
                            <Stack direction="row" spacing={1} justifyContent="center" flexWrap="wrap" useFlexGap>
                              <Chip
                                label={`Attempt cost: ${formatCredits(topicCost(t))}`}
                                sx={{
                                  background: 'rgba(52, 152, 219, 0.1)',
                                  color: '#2c3e50',
                                  border: '1px solid rgba(52, 152, 219, 0.25)',
                                  fontWeight: 'bold'
                                }}
                              />
                              <Chip
                                label="Start Learning"
                                sx={{
                                  background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
                                  color: 'white',
                                  fontWeight: 'bold',
                                  boxShadow: '0 4px 8px rgba(255, 107, 53, 0.3)'
                                }}
                              />
                            </Stack>
                          </Stack>
                        </CardActionArea>
                      </Card>
                    </Zoom>
                  </Grid>
                ))}
              </Grid>
            </Box>
          </Slide>
        </Container>
      </Box>
    );
  }

  /* ---------- CATEGORIES under selected Topic ---------- */
  if (view === "categories" && plan) {
    return (
      <Box sx={{
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
        py: 4,
        pb: 14
      }}>
        <Container maxWidth="lg">
          {renderTopCreditBar()}
          <Fade in timeout={800}>
            <Box>
              <Paper sx={{
                p: 2,
                mb: 3,
                borderRadius: 3,
                background: 'white',
                border: '1px solid rgba(195, 207, 226, 0.3)',
                boxShadow: '0 4px 12px rgba(44, 62, 80, 0.08)'
              }}>
                <Breadcrumbs>
                  <Link
                    underline="hover"
                    onClick={goHome}
                    sx={{
                      cursor: "pointer",
                      display: 'flex',
                      alignItems: 'center',
                      gap: 1,
                      color: '#3498db',
                      '&:hover': { color: '#ff6b35' }
                    }}
                  >
                    <HomeIcon fontSize="small" />
                    Home
                  </Link>
                  <Link
                    underline="hover"
                    onClick={() => setView("topics")}
                    sx={{
                      cursor: "pointer",
                      color: '#3498db',
                      '&:hover': { color: '#ff6b35' }
                    }}
                  >
                    {selectedSuper}
                  </Link>
                  <Typography color="#2c3e50" sx={{ fontWeight: 'bold' }}>
                    {plan.topic_name}
                  </Typography>
                </Breadcrumbs>
              </Paper>

              <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 2 }}>
                <BookIcon sx={{ fontSize: 32, color: '#ff6b35' }} />
                <Typography variant="h4" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                  {plan.topic_name}
                </Typography>
              </Stack>

              <Alert
                severity="info"
                sx={{
                  mb: 3,
                  borderRadius: 2,
                  backgroundColor: 'rgba(52, 152, 219, 0.1)',
                  border: '1px solid rgba(52, 152, 219, 0.3)',
                  '& .MuiAlert-icon': { color: '#3498db' }
                }}
              >
                <Typography variant="body2" sx={{ color: '#2c3e50' }}>
                  Browse by category first. If a subtopic has no category, its own title becomes the category label.
                </Typography>
              </Alert>

              <Grid container spacing={3}>
                {categoryGroups().map((group, index) => (
                  <Grid item xs={12} sm={6} md={4} key={group.key}>
                    <Zoom in timeout={600 + index * 80}>
                      <Card sx={{
                        borderRadius: 4,
                        boxShadow: '0 12px 24px rgba(44, 62, 80, 0.1)',
                        background: 'white',
                        border: '1px solid rgba(195, 207, 226, 0.3)',
                        height: '100%',
                        transition: 'all 0.3s ease',
                        '&:hover': {
                          transform: 'translateY(-8px)',
                          boxShadow: '0 20px 40px rgba(255, 107, 53, 0.15)',
                          border: '1px solid rgba(255, 107, 53, 0.3)'
                        }
                      }}>
                        <CardActionArea onClick={() => openCategory(group.key)} sx={{ height: '100%', p: 3 }}>
                          <Stack spacing={2}>
                            <Avatar sx={{
                              width: 56,
                              height: 56,
                              background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
                              boxShadow: '0 6px 12px rgba(255, 107, 53, 0.3)'
                            }}>
                              <LightbulbIcon />
                            </Avatar>
                            <Typography variant="h6" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                              {group.title}
                            </Typography>
                            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                              <Chip label={`${group.conceptCount} concept${group.conceptCount === 1 ? '' : 's'}`} size="small" />
                              <Chip label={`${group.caseCount} case${group.caseCount === 1 ? '' : 's'}`} size="small" />
                              <Chip label={`${group.questionCount} question${group.questionCount === 1 ? '' : 's'}`} size="small" />
                            </Stack>
                            <Typography variant="body2" sx={{ color: '#34495e', opacity: 0.8 }}>
                              Open this category to view its subtopics, concepts, and case studies.
                            </Typography>
                          </Stack>
                        </CardActionArea>
                      </Card>
                    </Zoom>
                  </Grid>
                ))}
              </Grid>
            </Box>
          </Fade>
        </Container>

        {renderLessonFooter("Back to Topics", () => {
          if (!enforceLock("topics")) setView("topics");
        })}

        {activeSessionDialog}
      </Box>
    );
  }

  /* ---------- CONCEPT page for a loaded topic ---------- */
  if (view === "concept" && plan) {
    return (
      <Box sx={{
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
        py: 4,
        pb: 14
      }}>
        <Container maxWidth="lg">
          {renderTopCreditBar()}
          <Fade in timeout={800}>
            <Paper sx={{
              p: 4,
              borderRadius: 4,
              boxShadow: '0 16px 32px rgba(44, 62, 80, 0.1)',
              background: 'white',
              border: '1px solid rgba(195, 207, 226, 0.3)'
            }}>
              <Paper sx={{
                p: 2,
                mb: 3,
                borderRadius: 2,
                background: 'linear-gradient(135deg, rgba(52, 152, 219, 0.1) 0%, rgba(52, 152, 219, 0.05) 100%)',
                border: '1px solid rgba(52, 152, 219, 0.2)'
              }}>
                <Breadcrumbs>
                  <Link
                    underline="hover"
                    onClick={goHome}
                    sx={{
                      cursor: "pointer",
                      display: 'flex',
                      alignItems: 'center',
                      gap: 1,
                      color: '#3498db',
                      '&:hover': { color: '#ff6b35' }
                    }}
                  >
                    <HomeIcon fontSize="small" />
                    Home
                  </Link>
                  <Link
                    underline="hover"
                    onClick={() => setView("topics")}
                    sx={{
                      cursor: "pointer",
                      color: '#3498db',
                      '&:hover': { color: '#ff6b35' }
                    }}
                  >
                    {selectedSuper}
                  </Link>
                  <Link
                    underline="hover"
                    onClick={() => setView("categories")}
                    sx={{
                      cursor: "pointer",
                      color: '#3498db',
                      '&:hover': { color: '#ff6b35' }
                    }}
                  >
                    {plan.topic_name}
                  </Link>
                  <Typography color="#2c3e50" sx={{ fontWeight: 'bold' }}>
                    {currentCategoryGroup()?.title}
                  </Typography>
                </Breadcrumbs>
              </Paper>

              <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 2 }}>
                <LightbulbIcon sx={{ fontSize: 32, color: '#ff6b35' }} />
                <Typography variant="h4" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                  {currentCategoryGroup()?.title}
                </Typography>
              </Stack>

              <Alert
                severity="info"
                sx={{
                  mb: 3,
                  borderRadius: 2,
                  backgroundColor: 'rgba(52, 152, 219, 0.1)',
                  border: '1px solid rgba(52, 152, 219, 0.3)',
                  '& .MuiAlert-icon': { color: '#3498db' }
                }}
              >
                <Typography variant="body2" sx={{ color: '#2c3e50' }}>
                  📚 Review the core concepts below to build a strong foundation. When you're ready, proceed to interactive questions!
                </Typography>
              </Alert>

              <Divider sx={{ mb: 3, borderColor: 'rgba(195, 207, 226, 0.5)' }} />

              <Grid container spacing={3}>
                {currentCategoryItems().map((st, index) => (
                  <Grid item xs={12} key={st.subtopic_id}>
                    <Slide direction="up" in timeout={600 + index * 200}>
                      <Paper sx={{
                        p: 3,
                        borderRadius: 3,
                        border: '2px solid transparent',
                        background: 'linear-gradient(white, white) padding-box, linear-gradient(135deg, #ff6b35, #f7931e) border-box',
                        transition: 'all 0.3s ease',
                        '&:hover': {
                          transform: 'translateY(-4px)',
                          boxShadow: '0 15px 30px rgba(255, 107, 53, 0.2)'
                        }
                      }}>
                        <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 2 }}>
                          <Avatar sx={{
                            width: 32,
                            height: 32,
                            background: 'linear-gradient(135deg, #ff6b35, #f7931e)',
                            fontSize: 14,
                            fontWeight: 'bold',
                            boxShadow: '0 4px 8px rgba(255, 107, 53, 0.3)'
                          }}>
                            {index + 1}
                          </Avatar>
                          <Typography variant="h6" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                            {st.subtopic_title}
                          </Typography>
                          <Chip
                            size="small"
                            label={st.is_case ? "Case Study" : "Concept"}
                            sx={{
                              background: st.is_case ? 'rgba(155, 89, 182, 0.12)' : 'rgba(255, 107, 53, 0.12)',
                              color: st.is_case ? '#8e44ad' : '#ff6b35',
                              fontWeight: 'bold'
                            }}
                          />
                        </Stack>
                        <Typography sx={{
                          whiteSpace: "pre-line",
                          lineHeight: 1.7,
                          color: '#34495e'
                        }}>
                          {st.concept}
                        </Typography>
                      </Paper>
                    </Slide>
                  </Grid>
                ))}
              </Grid>
            </Paper>
          </Fade>
        </Container>

        {renderLessonFooter("Back to Categories", () => setView("categories"))}

        {activeSessionDialog}
      </Box>
    );
  }

  /* ---------- QUESTIONS view ---------- */
  if (view === "questions" && plan) {
    return (
      <Box sx={{
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
        py: 2
      }}>
        <Container maxWidth="lg">
          {renderTopCreditBar()}
          {!finished && (
            <Fade in timeout={600}>
              <Paper sx={{
                borderRadius: 4,
                boxShadow: '0 20px 40px rgba(44, 62, 80, 0.1)',
                background: 'white',
                border: '1px solid rgba(195, 207, 226, 0.3)',
                overflow: 'hidden'
              }}>
                <Box sx={{
                  background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
                  p: 3,
                  color: 'white'
                }}>
                  <Paper sx={{
                    p: 1.5,
                    mb: 2,
                    borderRadius: 2,
                    background: 'rgba(255,255,255,0.15)',
                    border: '1px solid rgba(255,255,255,0.2)'
                  }}>
                    <Breadcrumbs sx={{ '& .MuiBreadcrumbs-separator': { color: 'white' } }}>
                      <Link
                        underline="hover"
                        onClick={goHome}
                        sx={{
                          cursor: "pointer",
                          color: 'white',
                          display: 'flex',
                          alignItems: 'center',
                          gap: 1,
                          '&:hover': { opacity: 0.8 }
                        }}
                      >
                        <HomeIcon fontSize="small" />
                        Home
                      </Link>
                      <Link
                        underline="hover"
                        onClick={() => setView("topics")}
                        sx={{
                          cursor: "pointer",
                          color: 'white',
                          '&:hover': { opacity: 0.8 }
                        }}
                      >
                        {selectedSuper}
                      </Link>
                      <Link
                        underline="hover"
                        onClick={openCurrentCategory}
                        sx={{
                          cursor: "pointer",
                          color: 'white',
                          '&:hover': { opacity: 0.8 }
                        }}
                      >
                        {plan.topic_name}
                      </Link>
                      <Typography sx={{ color: 'white', fontWeight: 'bold' }}>Questions</Typography>
                    </Breadcrumbs>
                  </Paper>

                  <Stack direction="row" alignItems="center" justifyContent="space-between">
                    <Stack direction="row" alignItems="center" spacing={2}>
                      <QuizIcon sx={{ fontSize: 28 }} />
                      <Typography variant="h5" sx={{ fontWeight: 'bold' }}>
                        {currentSub().subtopic_title}
                      </Typography>
                    </Stack>
                    <Chip
                      label={`${Math.round(getProgress())}% Complete`}
                      sx={{
                        background: 'rgba(255,255,255,0.2)',
                        color: 'white',
                        fontWeight: 'bold',
                        border: '1px solid rgba(255,255,255,0.3)'
                      }}
                    />
                  </Stack>

                  <LinearProgress
                    variant="determinate"
                    value={getProgress()}
                    sx={{
                      mt: 2,
                      height: 8,
                      borderRadius: 4,
                      background: 'rgba(255,255,255,0.2)',
                      '& .MuiLinearProgress-bar': {
                        background: 'linear-gradient(90deg, #2ed573 0%, #1dd1a1 100%)',
                        borderRadius: 4
                      }
                    }}
                  />
                </Box>

                <Box sx={{ p: 3 }}>
                  <Tabs
                    value={tab}
                    onChange={(_, v) => setTab(v)}
                    sx={{
                      mb: 3,
                      '& .MuiTab-root': {
                        fontWeight: 'bold',
                        minHeight: 48,
                        color: '#34495e'
                      },
                      '& .Mui-selected': {
                        color: '#ff6b35 !important'
                      },
                      '& .MuiTabs-indicator': {
                        backgroundColor: '#ff6b35'
                      }
                    }}
                  >
                    <Tab icon={<BookIcon />} label={isCase ? "Case Study" : "Concept"} />
                    <Tab icon={<QuizIcon />} label="Question" />
                    <Tab icon={<TimelineIcon />} label="References" />
                  </Tabs>

                  {tab === 0 && (
                    <Slide direction="right" in timeout={400}>
                      <Paper sx={{
                        p: 3,
                        borderRadius: 3,
                        background: 'linear-gradient(135deg, rgba(52, 152, 219, 0.1) 0%, rgba(52, 152, 219, 0.05) 100%)',
                        border: '1px solid rgba(52, 152, 219, 0.2)'
                      }}>
                        <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 2 }}>
                          <LightbulbIcon sx={{ color: '#3498db' }} />
                          <Typography variant="h6" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                            {isCase ? "Case Study Vignette" : "Core Concept"}
                            </Typography>
                        </Stack>
                        <Typography sx={{
                          whiteSpace: "pre-line",
                          lineHeight: 1.8,
                          fontSize: '1.1rem',
                          color: '#34495e'
                        }}>
                          {currentSub().concept}
                        </Typography>
                      </Paper>
                    </Slide>
                  )}

                  {tab === 1 && mode === "question" && (
                    <Slide direction="left" in timeout={400}>
                      <Box>
                        <Paper sx={{
                          p: 3,
                          mb: 3,
                          borderRadius: 3,
                          background: 'rgba(245, 247, 250, 0.7)',
                          border: '1px solid rgba(195, 207, 226, 0.3)'
                        }}>
                          <Typography variant="h6" sx={{ mb: 2, fontWeight: 'bold', color: '#2c3e50' }}>
                            Question
                          </Typography>
                          <Typography sx={{
                            mb: 3,
                            whiteSpace: "pre-line",
                            fontSize: '1.1rem',
                            lineHeight: 1.6,
                            color: '#34495e'
                          }}>
                            {currentStem()}
                          </Typography>

                          <RadioGroup
                            value={choice}
                            onChange={e => setChoice(e.target.value)}
                            sx={{ gap: 1 }}
                          >
                            {currentQ().choices.map(c => (
                              <Paper
                                key={c.choice_index}
                                sx={{
                                  p: 2,
                                  borderRadius: 2,
                                  border: choice === String(c.choice_index) ? '2px solid #ff6b35' : '2px solid transparent',
                                  background: choice === String(c.choice_index) ? 'rgba(255, 107, 53, 0.1)' : 'white',
                                  transition: 'all 0.2s ease',
                                  cursor: awaitingAutoNext ? 'not-allowed' : 'pointer',
                                  opacity: awaitingAutoNext ? 0.6 : 1,
                                  '&:hover': awaitingAutoNext ? {} : {
                                    border: '2px solid #ff6b35',
                                    background: 'rgba(255, 107, 53, 0.05)'
                                  }
                                }}
                                onClick={() => { if (!awaitingAutoNext) setChoice(String(c.choice_index)); }}
                              >
                                <FormControlLabel
                                  value={String(c.choice_index)}
                                  control={<Radio sx={{ color: '#ff6b35' }} disabled={awaitingAutoNext} />}
                                  label={c.choice_text}
                                  sx={{
                                    width: '100%',
                                    margin: 0,
                                    '& .MuiFormControlLabel-label': {
                                      fontSize: '1rem',
                                      fontWeight: choice === String(c.choice_index) ? 'bold' : 'normal',
                                      color: '#34495e'
                                    }
                                  }}
                                />
                              </Paper>
                            ))}
                          </RadioGroup>

                          {result === "incorrect" && (
                            <Zoom in timeout={300}>
                              <Alert
                                severity="error"
                                sx={{
                                  mt: 2,
                                  borderRadius: 2,
                                  backgroundColor: 'rgba(231, 76, 60, 0.1)',
                                  border: '1px solid rgba(231, 76, 60, 0.3)',
                                  '& .MuiAlert-icon': { color: '#e74c3c' }
                                }}
                              >
                                <Stack direction="row" alignItems="center" spacing={1} sx={{ flexWrap: 'wrap' }}>
                                  <CancelIcon />
                                  <Typography fontWeight="bold" sx={{ color: '#2c3e50' }}>
                                    Incorrect – Give it another try!
                                  </Typography>
                                  {awaitingAutoNext && (
                                    <Typography sx={{ color: '#2c3e50', ml: 1 }}>
                                      Auto-moving to Next Try in {autoNextIn}s…
                                    </Typography>
                                  )}
                                </Stack>
                              </Alert>
                            </Zoom>
                          )}

                          {/* NEW: Rationale for the attempted option */}
                          {showRationale && lastChosenIdx !== null && (
                            <Paper sx={{
                              p: 2, mt: 2, borderRadius: 2,
                              border: result === "correct" ? '1px solid rgba(46, 213, 115, 0.6)'
                                                        : '1px solid rgba(231, 76, 60, 0.6)',
                              background: result === "correct" ? 'rgba(46, 213, 115, 0.08)'
                                                              : 'rgba(231, 76, 60, 0.08)'
                            }}>
                              <Typography variant="subtitle1" sx={{ fontWeight: 'bold', mb: 1 }}>
                                {result === "correct" ? "Why this is correct" : "Why this isn’t correct"}
                              </Typography>
                              <Typography sx={{ whiteSpace: 'pre-line', lineHeight: 1.6 }}>
                                {(currentQ()?.choices || []).find(c => c.choice_index === lastChosenIdx)?.rationale || "—"}
                              </Typography>
                            </Paper>
                          )}

                          {/* NEW: Next Try button when incorrect and variants remain */}
                          {showRationale && result === "incorrect" && attemptIdx < (currentQ()?.variants?.length || 0) && (
                            <Stack direction="row" spacing={2} sx={{ mt: 2 }}>
                              <Button variant="contained" onClick={nextTry}>
                                Next Try
                              </Button>
                            </Stack>
                          )}

                          <Stack direction="row" justifyContent="center" sx={{ mt: 3 }}>
                            <Button
                              variant="contained"
                              disabled={choice === null || awaitingAutoNext}
                              onClick={submit}
                              sx={{
                                borderRadius: 3,
                                px: 4,
                                py: 1.5,
                                background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
                                boxShadow: '0 8px 20px rgba(255, 107, 53, 0.3)',
                                fontWeight: 'bold',
                                fontSize: '1.1rem',
                                '&:hover': {
                                  boxShadow: '0 12px 30px rgba(255, 107, 53, 0.4)',
                                  background: 'linear-gradient(135deg, #f7931e 0%, #ff6b35 100%)'
                                },
                                '&:disabled': {
                                  background: '#bdc3c7',
                                  boxShadow: 'none'
                                }
                              }}
                            >
                              Submit Answer
                            </Button>
                          </Stack>
                        </Paper>
                      </Box>
                    </Slide>
                  )}

                  {tab === 1 && mode === "explain" && currentQ() && (
                    <Slide direction="up" in timeout={500}>
                      <Box>
                        {/* Final review panel with all choices (green correct, chosen-wrong red) */}
                        {(() => {
                          const correctIdxMain = currentQ()?.correct_choice_index ?? 0;
                          const attemptsForThisQ = history.filter(a => a.question_id === currentQ()?.question_id);
                          const wrongChosenSet = new Set(attemptsForThisQ.filter(a => !a.correct).map(a => a.chosen_index));

                          return (
                            <Paper sx={{ p: 3, mb: 3, borderRadius: 3, border: '1px solid rgba(52, 152, 219, 0.3)' }}>
                              <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 2 }}>
                                <PsychologyIcon sx={{ color: '#3498db' }} />
                                <Typography variant="h6" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                                  Review answers for this question
                                </Typography>
                              </Stack>

                              <Typography sx={{ mb: 2, fontWeight: 'bold' }}>
                                {currentQ()?.stem}
                              </Typography>

                              <Stack spacing={1.5}>
                                {(currentQ()?.choices || []).map((c) => {
                                  const isCorrectChoice = c.choice_index === correctIdxMain;
                                  const wasPickedAndWrong = wrongChosenSet.has(c.choice_index);
                                  return (
                                    <Paper key={c.choice_index} sx={{
                                      p: 2, borderRadius: 2,
                                      border: isCorrectChoice
                                        ? '1px solid rgba(46, 213, 115, 0.7)'
                                        : (wasPickedAndWrong ? '1px solid rgba(231, 76, 60, 0.7)' : '1px solid rgba(189,195,199,0.6)'),
                                      background: isCorrectChoice
                                        ? 'rgba(46, 213, 115, 0.08)'
                                        : (wasPickedAndWrong ? 'rgba(231, 76, 60, 0.08)' : 'rgba(236, 240, 241, 0.3)')
                                    }}>
                                      <Stack direction="row" spacing={1} alignItems="center">
                                        {isCorrectChoice && <CheckIcon sx={{ color: '#2ed573' }} />}
                                        {wasPickedAndWrong && <CancelIcon sx={{ color: '#e74c3c' }} />}
                                        <Typography sx={{ fontWeight: 600 }}>
                                          {String.fromCharCode(65 + c.choice_index)}. {c.choice_text}
                                        </Typography>
                                      </Stack>
                                      {c.rationale && (
                                        <Typography sx={{ mt: 1.0, whiteSpace: 'pre-line', color: '#34495e' }}>
                                          {c.rationale}
                                        </Typography>
                                      )}
                                    </Paper>
                                  );
                                })}
                              </Stack>
                            </Paper>
                          );
                        })()}

                        <Paper sx={{
                          p: 3,
                          mb: 3,
                          borderRadius: 3,
                          background: 'linear-gradient(135deg, rgba(46, 213, 115, 0.1) 0%, rgba(29, 209, 161, 0.1) 100%)',
                          border: '1px solid rgba(46, 213, 115, 0.3)'
                        }}>
                          <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 2 }}>
                            <LightbulbIcon sx={{ color: '#2ed573' }} />
                            <Typography variant="h6" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                              Explanation
                            </Typography>
                          </Stack>
                          <Typography sx={{
                            whiteSpace: "pre-line",
                            lineHeight: 1.7,
                            fontSize: '1.1rem',
                            color: '#34495e'
                          }}>
                            {currentQ().explanation}
                          </Typography>
                        </Paper>



                        <Stack direction="row" justifyContent="center">
                          <Button
                            variant="contained"
                            onClick={proceed}
                            endIcon={<ArrowForwardIcon />}
                            sx={{
                              borderRadius: 3,
                              px: 4,
                              py: 1.5,
                              background: 'linear-gradient(135deg, #2ed573 0%, #1dd1a1 100%)',
                              boxShadow: '0 8px 20px rgba(46, 213, 115, 0.3)',
                              fontWeight: 'bold',
                              fontSize: '1.1rem',
                              '&:hover': {
                                boxShadow: '0 12px 30px rgba(46, 213, 115, 0.4)',
                                background: 'linear-gradient(135deg, #1dd1a1 0%, #2ed573 100%)'
                              }
                            }}
                          >
                            Continue Learning
                          </Button>
                        </Stack>
                      </Box>
                    </Slide>
                  )}

                  {tab === 2 && (
                    <Slide direction="left" in timeout={400}>
                      <Paper sx={{
                        p: 3,
                        borderRadius: 3,
                        background: 'linear-gradient(135deg, rgba(155, 89, 182, 0.1) 0%, rgba(155, 89, 182, 0.05) 100%)',
                        border: '1px solid rgba(155, 89, 182, 0.3)'
                      }}>
                        <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 3 }}>
                          <TimelineIcon sx={{ color: '#9b59b6' }} />
                          <Typography variant="h6" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                            Additional References
                          </Typography>
                        </Stack>
                        {(currentSub().references || []).length === 0 ? (
                          <Typography sx={{ fontStyle: 'italic', color: '#7f8c8d' }}>
                            No additional references available for this topic.
                          </Typography>
                        ) : (
                          (currentSub().references || []).map((r, i) => (
                            <Paper key={i} sx={{
                              p: 2,
                              mb: 2,
                              borderRadius: 2,
                              border: '1px solid rgba(189, 195, 199, 0.3)',
                              background: 'white'
                            }}>
                              <Typography
  sx={{
    whiteSpace: "pre-line",
    lineHeight: 1.6,
    color: r.citation_link ? '#2980b9' : '#34495e',
    cursor: r.citation_link ? 'pointer' : 'default',
  }}
  onClick={() => {
    if (r.citation_link) window.open(r.citation_link, "_blank");
  }}
>
  📖 {r.excerpt ?? r}
</Typography>

                            </Paper>
                          ))
                        )}
                      </Paper>
                    </Slide>
                  )}
                </Box>
              </Paper>
            </Fade>
          )}

          {finished && (
            <Zoom in timeout={800}>
              <Paper sx={{
                borderRadius: 4,
                boxShadow: '0 20px 40px rgba(44, 62, 80, 0.1)',
                background: 'white',
                border: '1px solid rgba(195, 207, 226, 0.3)',
                overflow: 'hidden'
              }}>
                <Box sx={{
                  background: 'linear-gradient(135deg, #2ed573 0%, #1dd1a1 100%)',
                  p: 4,
                  color: 'white',
                  textAlign: 'center'
                }}>
                  <TrophyIcon sx={{ fontSize: 64, mb: 2 }} />
                  <Typography variant="h4" sx={{ fontWeight: 'bold', mb: 1 }}>
                    Congratulations!
                  </Typography>
                  <Typography variant="h6" sx={{ opacity: 0.9 }}>
                    You've completed your learning session
                  </Typography>
                </Box>

                <Box sx={{ p: 4 }}>
                  <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold', textAlign: 'center', color: '#2c3e50' }}>
                    Your Learning Report
                  </Typography>

                  {loadingReport && (
                    <Stack direction="row" spacing={3} justifyContent="center" sx={{ py: 4 }}>
                      <CircularProgress size={32} sx={{ color: '#ff6b35' }} />
                      <Typography variant="h6" sx={{ color: '#ff6b35' }}>
                        Generating your personalized report...
                      </Typography>
                    </Stack>
                  )}

                  {!loadingReport && reportMd && (
                    <Fade in timeout={600}>
                      <Box>
                        <Paper sx={{
                          p: 3,
                          mb: 3,
                          borderRadius: 3,
                          background: 'linear-gradient(135deg, rgba(255, 107, 53, 0.05) 0%, rgba(247, 147, 30, 0.05) 100%)',
                          border: '1px solid rgba(255, 107, 53, 0.2)'
                        }}>
                          <Box
                            sx={{
                              whiteSpace: "pre-line",
                              '& h1, & h2, & h3': {
                                color: '#ff6b35',
                                fontWeight: 'bold'
                              },
                              '& p': {
                                lineHeight: 1.7,
                                marginBottom: 2,
                                color: '#34495e'
                              },
                              '& ul': {
                                paddingLeft: 3,
                                '& li': {
                                  color: '#34495e'
                                }
                              }
                            }}
                            dangerouslySetInnerHTML={{ __html: marked.parse(reportMd) }}
                          />
                        </Paper>

                        <Stack direction="row" spacing={3} justifyContent="center" flexWrap="wrap">
                          <Button
                            variant="outlined"
                            onClick={() => downloadMarkdown(`session_report_${sessionId}.md`, reportMd)}
                            startIcon={<DownloadIcon />}
                            sx={{
                              borderRadius: 3,
                              px: 3,
                              py: 1.5,
                              borderColor: '#9b59b6',
                              color: '#9b59b6',
                              fontWeight: 'bold',
                              '&:hover': {
                                borderColor: '#9b59b6',
                                background: 'rgba(155, 89, 182, 0.1)'
                              }
                            }}
                          >
                            Download Report
                          </Button>

                          <Button
                            variant="outlined"
                            onClick={() => setView("categories")}
                            startIcon={<BookIcon />}
                            sx={{
                              borderRadius: 3,
                              px: 3,
                              py: 1.5,
                              borderColor: '#3498db',
                              color: '#3498db',
                              fontWeight: 'bold',
                              '&:hover': {
                                borderColor: '#3498db',
                                background: 'rgba(52, 152, 219, 0.1)'
                              }
                            }}
                          >
                            Review Categories
                          </Button>

                          <Button
                            variant="contained"
                            onClick={goHome}
                            startIcon={<HomeIcon />}
                            sx={{
                              borderRadius: 3,
                              px: 4,
                              py: 1.5,
                              background: 'linear-gradient(135deg, #ff6b35 0%, #f7931e 100%)',
                              boxShadow: '0 8px 20px rgba(255, 107, 53, 0.3)',
                              fontWeight: 'bold',
                              '&:hover': {
                                boxShadow: '0 12px 30px rgba(255, 107, 53, 0.4)',
                                background: 'linear-gradient(135deg, #f7931e 0%, #ff6b35 100%)'
                              }
                            }}
                          >
                            New Learning Journey
                          </Button>
                        </Stack>
                      </Box>
                    </Fade>
                  )}
                </Box>
              </Paper>
            </Zoom>
          )}
        </Container>

        {activeSessionDialog}
      </Box>
    );
  }

  /* ---------- DASHBOARD VIEW ---------- */
  if (view === "dashboard") {
    return (
      <Box sx={{
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
        py: 4
      }}>
        <Container maxWidth="lg">
          {renderTopCreditBar()}
          <Fade in timeout={800}>
            <Box>
              <Paper sx={{
                p: 2,
                mb: 3,
                borderRadius: 3,
                background: 'white',
                border: '1px solid rgba(195, 207, 226, 0.3)',
                boxShadow: '0 4px 12px rgba(44, 62, 80, 0.08)'
              }}>
                <Breadcrumbs>
                  <Link
                    underline="hover"
                    onClick={goHome}
                    sx={{
                      cursor: "pointer",
                      display: 'flex',
                      alignItems: 'center',
                      gap: 1,
                      color: '#3498db',
                      '&:hover': { color: '#ff6b35' }
                    }}
                  >
                    <HomeIcon fontSize="small" />
                    Home
                  </Link>
                  <Typography color="#2c3e50" sx={{ fontWeight: 'bold' }}>
                    Dashboard
                  </Typography>
                </Breadcrumbs>
              </Paper>

              <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 4 }}>
                <DashboardIcon sx={{ fontSize: 40, color: '#ff6b35' }} />
                <Typography variant="h4" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                  Your Dashboard
                </Typography>
              </Stack>

              {dashboardRows.length === 0 ? (
                <Paper sx={{
                  p: 4,
                  borderRadius: 4,
                  textAlign: 'center',
                  background: 'white',
                  border: '1px solid rgba(195, 207, 226, 0.3)'
                }}>
                  <Typography variant="h6" sx={{ color: '#34495e' }}>
                    No sessions yet. Start learning to see your progress here!
                  </Typography>
                </Paper>
              ) : (
                <Grid container spacing={3}>
                  {dashboardRows.map((row, index) => (
                    <Grid item xs={12} key={row.session_id}>
                      <Zoom in timeout={600 + index * 100}>
                        <Paper sx={{
                          p: 3,
                          borderRadius: 3,
                          background: 'white',
                          border: '1px solid rgba(195, 207, 226, 0.3)',
                          boxShadow: '0 8px 20px rgba(44, 62, 80, 0.08)',
                          transition: 'all 0.3s ease',
                          '&:hover': {
                            boxShadow: '0 12px 30px rgba(44, 62, 80, 0.15)'
                          }
                        }}>
                          <Grid container spacing={2} alignItems="center">
                            <Grid item xs={12} sm={6}>
                              <Stack spacing={1}>
                                <Stack direction="row" alignItems="center" spacing={1}>
                                  <BookIcon sx={{ color: '#ff6b35' }} />
                                  <Typography variant="h6" sx={{ fontWeight: 'bold', color: '#2c3e50' }}>
                                    {row.topic_name}
                                  </Typography>
                                </Stack>
                                <Typography variant="body2" sx={{ color: '#34495e' }}>
                                  Session ID: {row.session_id}
                                </Typography>
                                <Chip
                                  label={dashboardStatus(row.status).label}
                                  sx={{
                                    width: 'fit-content',
                                    background: dashboardStatus(row.status).background,
                                    color: 'white',
                                    fontWeight: 'bold'
                                  }}
                                />
                                {row.score_pct != null && (
                                  <Box sx={{ mt: 1 }}>
                                    <Typography variant="body2" sx={{ mb: 0.5, fontWeight: 'bold', color: '#2c3e50' }}>
                                      Score: {row.score_pct}%
                                    </Typography>
                                    <LinearProgress
                                      variant="determinate"
                                      value={row.score_pct}
                                      sx={{
                                        borderRadius: 2,
                                        height: 6,
                                        background: 'rgba(195, 207, 226, 0.3)',
                                        '& .MuiLinearProgress-bar': {
                                          background: 'linear-gradient(90deg, #ff6b35 0%, #f7931e 100%)',
                                          borderRadius: 2
                                        }
                                      }}
                                    />
                                  </Box>
                                )}
                              </Stack>
                            </Grid>
                            <Grid item xs={12} sm={6}>
                              <Stack direction="row" spacing={2} justifyContent="flex-end">
                                <Button
                                  variant="outlined"
                                  onClick={() => startTopic(row.topic_id)}
                                  sx={{
                                    borderRadius: 2,
                                    px: 2,
                                    py: 1,
                                    borderColor: '#3498db',
                                    color: '#3498db',
                                    fontWeight: 'bold',
                                    textTransform: 'none',
                                    '&:hover': {
                                      borderColor: '#3498db',
                                      background: 'rgba(52, 152, 219, 0.1)'
                                    }
                                  }}
                                >
                                  Take Again
                                </Button>
                                {row.status === "completed" && (
                                  <Button
                                    variant="contained"
                                    onClick={async () => {
                                      try {
                                        const { data: reportData } = await axios.get(`/api/api/report/${row.session_id}`);
                                        const { data: planJson } = await axios.get(`/api/api/lesson/${row.topic_id}/${USER_ID}`);
                                        const groups = buildCategoryGroups(planJson);
                                        setReportMd(reportData.markdown);
                                        setPlan(planJson);
                                        setTopicId(row.topic_id);
                                        setSelectedSuper(planJson?.supertopic || "");
                                        setSelectedCategory(groups[0]?.key || "");
                                        setFinished(true);
                                        // Don't set sessionId - this is a completed session, not an active one
                                        // setSessionId(row.session_id);
                                        setView("questions");
                                      } catch (error) {
                                        console.error("Error fetching report:", error);
                                        alert("Failed to load performance report. Please try again.");
                                      } finally {
                                        setLoading(false);
                                      }
                                    }}
                                    sx={{
                                      borderRadius: 2,
                                      px: 2,
                                      py: 1,
                                      background: 'linear-gradient(135deg, #9b59b6 0%, #8e44ad 100%)',
                                      boxShadow: '0 4px 12px rgba(155, 89, 182, 0.3)',
                                      fontWeight: 'bold',
                                      textTransform: 'none',
                                      '&:hover': {
                                        boxShadow: '0 6px 18px rgba(155, 89, 182, 0.4)',
                                        background: 'linear-gradient(135deg, #8e44ad 0%, #9b59b6 100%)'
                                      }
                                    }}
                                  >
                                    Performance
                                  </Button>
                                )}
                              </Stack>
                            </Grid>
                          </Grid>
                        </Paper>
                      </Zoom>
                    </Grid>
                  ))}
                </Grid>
              )}
            </Box>
          </Fade>
        </Container>
      </Box>
    );
  }

  return (
    <Box sx={{
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      position: 'relative',
      minHeight: '100vh',
      background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)'
    }}>
      <Box sx={{ position: 'absolute', top: 24, right: 24 }}>
        {renderCreditBalance()}
      </Box>
      <Typography variant="h6" sx={{ color: '#2c3e50', fontWeight: 'bold' }}>
        Something went wrong. Please try again.
      </Typography>
    </Box>
  );
}
