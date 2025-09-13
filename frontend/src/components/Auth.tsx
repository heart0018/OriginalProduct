// Auth.tsx
import { ReactNode, useCallback, useEffect, useRef, useState } from "react";
import "./Auth.css";

declare global {
  interface Window {
    google?: {
      accounts: {
        id: {
          initialize: (opts: {
            client_id: string;
            callback: (resp: { credential: string }) => void;
            auto_select?: boolean;
            ux_mode?: "popup" | "redirect";
            nonce?: string;
          }) => void;

          renderButton: (
            el: HTMLElement,
            opts?: Record<string, unknown>
          ) => void;
          prompt: () => void;
          cancel: () => void;
          disableAutoSelect: () => void;
        };
      };
    };
  }
}

type User = any;

type Props = {
  children?: ReactNode;
  clientId?: string;
  onAuthenticated?: (user: User) => void | Promise<void>;
};

const Auth = ({ children, clientId, onAuthenticated }: Props) => {
  const [ready, setReady] = useState(false);
  const [authed, setAuthed] = useState<null | boolean>(null);
  const btnRef = useRef<HTMLDivElement | null>(null);
  const renderedRef = useRef(false);

  // 起動時のセッション確認
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch("http://localhost:3001/api/v1/session", {
          credentials: "include",
        });
        setAuthed(res.ok);
      } catch {
        setAuthed(false);
      }
    })();
  }, []);

  // Google認証成功時
  const handleSuccess = useCallback(
    async (idToken: string) => {
      try {
        const res = await fetch("http://localhost:3001/api/v1/auth/google", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify({ id_token: idToken }),
        });
        if (!res.ok) {
          setAuthed(false);
          return;
        }
        const user = await res.json().catch(() => ({}));
        setAuthed(true);
        await onAuthenticated?.(user);
      } catch {
        setAuthed(false);
      }
    },
    [onAuthenticated]
  );

  useEffect(() => {
    if (renderedRef.current) return;

    const init = () => {
      const g = window.google?.accounts?.id;
      if (!g || !btnRef.current) return;
      btnRef.current.innerHTML = "";
      g.initialize({
        client_id: import.meta.env.VITE_GOOGLE_CLIENT_ID!,

        callback: (resp: { credential: string }) =>
          handleSuccess(resp.credential),
      });
      g.renderButton(btnRef.current, {
        theme: "filled_blue",
        size: "large",
        text: "signin_with",
        shape: "circle",
        type: "standard",
      });
      renderedRef.current = true;
      setReady(true);
    };

    if (window.google?.accounts?.id) {
      init();
    } else {

      const onLoad = () => init();
      window.addEventListener("load", onLoad, { once: true });
      return () => window.removeEventListener("load", onLoad);
    }
  }, [handleSuccess]);


  const images = [
    "/images/bg1.png",
    "/images/bg2.png",
    "/images/bg3.png",
    "/images/bg4.png",
    "/images/bg5.png",
    "/images/bg6.png",
  ];


  if (authed) return <>{children}</>;

 
  const subText = "スワイプした先にあなたの行きたい場所がある";

  return (
    <section className="auth-hero">
      <div className="auth-bg" aria-hidden="true">
        <div className="auth-bg-track">
          {images.map((src, i) => (
            <img key={`a-${i}`} src={src} alt="" />
          ))}
        </div>
        <div className="auth-bg-track auth-bg-track--clone">
          {images.map((src, i) => (
            <img key={`b-${i}`} src={src} alt="" />
          ))}
        </div>
      </div>
      <div className="auth-overlay">
        <div className="auth-card">
          <h1 className="auth-title">Swipe</h1>

          {/* 1文字ずつspan化（改行はブラウザ任せでOK） */}
          <p className="auth-sub">
            {subText.split("").map((ch, i) => (
              <span
                key={i}
                className="auth-sub__char"
                style={{ ["--i" as any]: i } as any}
              >
                {ch}
              </span>
            ))}
          </p>

          <div className="auth-google" ref={btnRef} />
          {!ready && <p style={{ marginTop: 8 }}>Loading Google Sign-In…</p>}
        </div>
      </div>
    </section>
  );
};

export default Auth;
