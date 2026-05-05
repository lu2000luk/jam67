export default {
  async fetch(request, env, ctx) {
    const { API_KEY, BASE_URL } = env; // https://plate.lu2000luk.com/
    const url = new URL(request.url);

    if (url.pathname === "/push" && request.method === "POST") {
      if (!request.body) {
        return new Response("No body provided", { status: 400 });
      }
      const id = url.searchParams.get("id");
      if (!id) {
        return new Response("No id provided", { status: 400 });
      }

      await fetch(`${BASE_URL}/pubsub/${id}/publish`, {
        method: "POST",
        headers: {
          Authorization: API_KEY,
        },
        body: request.body,
      });
      return new Response(id, { headers: { "Content-Type": "text/plain" } });
    }

    if (url.pathname.startsWith("/socket") && request.method === "GET") {
      const id = url.searchParams.get("id");
      if (!id) {
        return new Response("No id provided", { status: 400 });
      }

      const createResp = await fetch(
        `${BASE_URL}/pubsub/${id}/client?max_dur=0&expiry=300000&max_uses=1`,
        {
          method: "GET",
          headers: {
            Authorization: API_KEY,
          },
        },
      );

      const createData = await createResp.json();

      console.log(createData);

      const wsBase = BASE_URL.replace(/^http/, "ws");

      return new Response(
        `${wsBase}${createData.data.path.substring(createData.data.path.indexOf("/", 1))}`,
        {
          headers: { "Content-Type": "text/plain" },
        },
      );
    }

    return new Response("[Jam67] No such endpoint", { status: 404 });
  },
};
