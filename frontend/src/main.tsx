import React from "react";
import ReactDOM from "react-dom/client";
import ContestLab from "./ContestLab";
import ContestSession from "./ContestSession";
ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ContestSession>
      <ContestLab />
    </ContestSession>
  </React.StrictMode>,
);
