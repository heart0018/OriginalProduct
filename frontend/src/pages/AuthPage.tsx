// AuthPages.tsx
import { useState } from "react";
import Auth from "../components/Auth";
import SwipePage from "./SwipePage";

type User = any;

const AuthPage = () => {
  const [user, setUser] = useState<User | null>(null);
  return (
    <Auth onAuthenticated={setUser}>
       <SwipePage /* userを使わないなら渡さなくてOK */ />
    </Auth>
  );
};

export default AuthPage;
