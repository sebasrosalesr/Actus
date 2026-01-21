import { useState } from "react";
import ActusChat from "./components/ActusChat";
import Login from "./components/Login";

export default function App() {
  const [userEmail, setUserEmail] = useState<string | null>(() => {
    return localStorage.getItem("actusUserEmail");
  });

  const handleLogin = (email: string) => {
    localStorage.setItem("actusUserEmail", email);
    setUserEmail(email);
  };

  const handleLogout = () => {
    localStorage.removeItem("actusUserEmail");
    setUserEmail(null);
  };

  if (!userEmail) {
    return <Login onLogin={handleLogin} />;
  }

  return <ActusChat userEmail={userEmail} onLogout={handleLogout} />;
}
