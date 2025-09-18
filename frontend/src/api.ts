export type CheckResponse = {
  valid: boolean;
  base_year?: number | null;
  missing?: string[];
};

export async function checkIFsFolder(path: string): Promise<CheckResponse> {
  const response = await fetch("http://localhost:8000/ifs/check", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ path })
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || "Request failed");
  }

  return response.json();
}
