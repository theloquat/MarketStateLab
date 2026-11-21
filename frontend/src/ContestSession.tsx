import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import "./contest.css";
type User = { username: string; role: string };
const Session = createContext<User>({ username: "", role: "viewer" });
export const useContestSession = () => useContext(Session);
export default function ContestSession({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null),
    [checking, setChecking] = useState(true),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  const check = async () => {
    try {
      const r = await fetch("/auth/me");
      if (r.ok) setUser(await r.json());
      else if (r.status !== 401) setError("登录服务暂不可用，请检查运行状态。");
    } catch {
      setError("无法连接实验室服务。");
    } finally {
      setChecking(false);
    }
  };
  useEffect(() => {
    document.title = "市场状态实验室";
    check();
    const expire = () => setUser(null);
    window.addEventListener("quantos-session-expired", expire);
    return () => window.removeEventListener("quantos-session-expired", expire);
  }, []);
  if (checking)
    return (
      <div className="lab-login">
        <h1>市场状态实验室</h1>
        <p>正在核验工作区会话…</p>
      </div>
    );
  if (!user)
    return (
      <div className="lab-login">
        <form
          onSubmit={async (e) => {
            e.preventDefault();
            setBusy(true);
            setError("");
            const f = new FormData(e.currentTarget);
            try {
              const r = await fetch("/auth/login", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  username: f.get("username"),
                  password: f.get("password"),
                }),
              });
              const d = await r.json();
              if (!r.ok) throw new Error(d.detail || "登录失败");
              setUser({ username: d.username, role: d.role });
            } catch (e) {
              setError(String(e));
            } finally {
              setBusy(false);
            }
          }}
        >
          <p className="lab-eyebrow">MARKET STATE LAB</p>
          <h1>登录市场状态实验室</h1>
          <p>连接真实数据、模型、预警与研究证据。</p>
          <label>
            用户名
            <input
              name="username"
              autoComplete="username"
              required
              defaultValue="admin"
            />
          </label>
          <label>
            密码
            <input
              name="password"
              type="password"
              autoComplete="current-password"
              required
            />
          </label>
          {error && (
            <p className="lab-error" role="alert">
              {error}
            </p>
          )}
          <button className="lab-button" disabled={busy}>
            {busy ? "正在验证…" : "登录工作区"}
          </button>
          <details>
            <summary>首次使用如何获取本机账户？</summary>
            <p>
              在项目目录运行 <code>./scripts/contest.sh credentials</code>
              。系统提供管理员、研究员、只读观察员三种角色。账户信息请勿录入演示视频。
            </p>
          </details>
        </form>
      </div>
    );
  return (
    <Session.Provider value={user}>
      <div className="lab-session-bar">
        <span>
          {user.username} ·{" "}
          {
            (
              {
                admin: "管理员",
                researcher: "研究员",
                viewer: "只读观察员",
              } as Record<string, string>
            )[user.role]
          }
        </span>
        <button
          onClick={async () => {
            await fetch("/auth/logout", { method: "POST" });
            setUser(null);
          }}
        >
          退出
        </button>
      </div>
      {children}
    </Session.Provider>
  );
}
