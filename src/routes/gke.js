import {Router} from 'express';
import {trackGKE} from "../controllers/gkeController";

const router = Router();

router.get('/track/', async (req, res) => {
  await trackGKE(req, res)
});

export default router;
